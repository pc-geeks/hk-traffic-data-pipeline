"""Command line:  hktraffic <command>   (or python -m hktraffic <command>)

Typical day:   hktraffic run-day --date 2026-10-28
Step by step:  unzip -> silver -> check -> gold -> serve
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from . import archive, quality, serve, spark_jobs, synthetic
from .config import Settings, get_settings


def _day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _days(start: date, end: date) -> list:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _jprint(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


# ------------------------------------------------------------------- commands
def cmd_unzip(args, s: Settings) -> int:
    found = archive.find_zips(s.landing, args.date)
    if not found["raw"]:
        print(f"no raw zip for {args.date} in {s.landing}", file=sys.stderr)
        return 1
    results = [archive.unzip_day(z, archive.day_dir(s.raw, "raw", args.date)) for z in found["raw"]]
    _jprint(results)
    return 0


def cmd_scan(args, s: Settings) -> int:
    found = archive.scan_landing(s.landing)
    for day, kinds in sorted(found["days"].items()):
        print(f"{day}  raw={len(kinds['raw'])}")
    print("detector zips:", [p.name for p in found["detectors"]])
    if found["unknown"]:
        print("NOT RECOGNISED:", [p.name for p in found["unknown"]])
    return 0


def _dimension_sources(args, s: Settings):
    """Explicit path wins; otherwise use the newest detector zip in the landing folder."""
    found = archive.scan_landing(s.landing)
    return args.detectors or (found["detectors"][-1] if found["detectors"] else None)


def cmd_dims(args, s: Settings) -> int:
    det = _dimension_sources(args, s)
    if not det:
        print("need a detector file in data/landing", file=sys.stderr)
        return 1
    print(f"detectors: {Path(det).name}")
    spark = spark_jobs.get_spark(s)
    try:
        _jprint(spark_jobs.build_dimensions(spark, s, archive.read_csv_bytes(det)))
    finally:
        spark.stop()
    return 0


def cmd_silver(args, s: Settings) -> int:
    spark = spark_jobs.get_spark(s)
    try:
        out = [spark_jobs.build_silver_lanes(spark, s, args.date)]
    finally:
        spark.stop()
    _save_metrics(s, out)
    _jprint(out)
    return 0


def _save_metrics(s: Settings, metrics_list: list) -> None:
    """Keep each day's quality metrics next to the data, so `serve` can load them into pipeline_run."""
    folder = s.silver / "_metrics"
    folder.mkdir(parents=True, exist_ok=True)
    for m in metrics_list:
        (folder / f"{m['kind']}_{m['day']}.json").write_text(json.dumps(m, default=str), encoding="utf-8")


def _known_detectors(spark, s: Settings):
    path = s.silver / "dim_detector"
    return spark.read.parquet(str(path)).count() if path.exists() else None


def cmd_check(args, s: Settings) -> int:
    spark = spark_jobs.get_spark(s)
    try:
        raw_files = spark_jobs._count_xml(s.raw / "raw" / f"dt={args.date:%Y-%m-%d}")
        m = spark_jobs.lane_metrics(spark, s, args.date, raw_files)
        m["detectors_known"] = _known_detectors(spark, s)
    finally:
        spark.stop()
    try:
        quality.check(m, s)
    except quality.DataQualityError as exc:
        print(f"DATA QUALITY FAILED: {exc}", file=sys.stderr)
        return 1
    _jprint(m)
    return 0


def cmd_gold(args, s: Settings) -> int:
    days = _days(args.start, args.end or args.start)
    spark = spark_jobs.get_spark(s)
    try:
        dh = spark_jobs.build_detector_hour(spark, s, days)
        spark_jobs.write_gold(dh, s, "detector_hour")
        all_days = _available_days(s)
        spark_jobs.write_gold(spark_jobs.build_hotspots(spark, s, all_days), s, "hotspots", partitioned=False, files=1)
        for d in days:
            others = [x for x in all_days if x != d]
            if len(others) >= s.anomaly_min_baseline_days:
                spark_jobs.write_gold(spark_jobs.build_anomalies(spark, s, d, all_days), s, "anomalies")
            else:
                print(f"skip anomalies for {d}: need >= {s.anomaly_min_baseline_days} other days, have {len(others)}")
    finally:
        spark.stop()
    return 0


def _available_days(s: Settings) -> list:
    base = s.gold / "detector_hour"
    return sorted(_day(p.name.split("=", 1)[1]) for p in base.glob("dt=*")) if base.exists() else []


def cmd_serve(args, s: Settings) -> int:
    spark = spark_jobs.get_spark(s)
    conn = None
    try:
        conn = serve.connect(s.dsn)
        serve.init_schema(conn, s.schema)
        counts = {}
        for table, src in (("dim_detector", s.silver / "dim_detector"),
                           ("fact_detector_hour", s.gold / "detector_hour"), ("hotspot", s.gold / "hotspots"),
                           ("anomaly_hour", s.gold / "anomalies")):
            if src.exists():
                rows = [r.asDict() for r in spark.read.parquet(str(src)).collect()]
                counts[table] = serve.upsert(conn, s.schema, table, rows)
        runs = sorted((s.silver / "_metrics").glob("*.json")) if (s.silver / "_metrics").exists() else []
        for path in runs:
            serve.record_run(conn, s.schema, json.loads(path.read_text(encoding="utf-8")))
        counts["pipeline_run"] = len(runs)
    finally:
        if conn is not None:
            conn.close()
        spark.stop()
    _jprint(counts)
    return 0


def cmd_export_bi(args, s: Settings) -> int:
    conn = serve.connect(s.dsn)
    try:
        serve.init_schema(conn, s.schema)      # (re)creates the views too
        _jprint(serve.export_views(conn, s.schema, args.out))
    finally:
        conn.close()
    return 0


def cmd_run_range(args, s: Settings) -> int:
    """Several days in ONE Spark session: unzip -> silver -> quality gate per day, then gold for the whole range.

    Starting Spark costs ~15 s, so doing it once matters. --delete-raw removes a day's extracted XML after its
    silver data is written (the zips in data/landing are kept), which saves ~1 GB of disk per day.
    """
    days = _days(args.start, args.end)
    spark = spark_jobs.get_spark(s)
    failed = []
    try:
        known = _known_detectors(spark, s)
        for day in days:
            found = archive.find_zips(s.landing, day)
            if not found["raw"]:
                print(f"{day}: missing raw zip, skipped")
                failed.append(str(day))
                continue
            for z in found["raw"]:
                archive.unzip_day(z, archive.day_dir(s.raw, "raw", day))
            lanes = spark_jobs.build_silver_lanes(spark, s, day)
            _save_metrics(s, [lanes])
            lanes["detectors_known"] = known
            print(f"{day}: raw files={lanes['files']} rows={lanes['rows']:,} flags={lanes['flags']}", flush=True)
            try:
                quality.check(lanes, s)
            except quality.DataQualityError as exc:
                print(f"{day}: QUALITY GATE FAILED: {exc}", file=sys.stderr)
                failed.append(str(day))
            if args.delete_raw:
                shutil.rmtree(archive.day_dir(s.raw, "raw", day), ignore_errors=True)
        good = [d for d in days if str(d) not in failed]
        if good:
            dh = spark_jobs.build_detector_hour(spark, s, good)
            spark_jobs.write_gold(dh, s, "detector_hour")
            all_days = _available_days(s)
            spark_jobs.write_gold(spark_jobs.build_hotspots(spark, s, all_days), s, "hotspots",
                                  partitioned=False, files=1)
            for d in good:
                if len([x for x in all_days if x != d]) >= s.anomaly_min_baseline_days:
                    spark_jobs.write_gold(spark_jobs.build_anomalies(spark, s, d, all_days), s, "anomalies")
                else:
                    print(f"skip anomalies for {d}: not enough baseline days")
    finally:
        spark.stop()
    if failed:
        print(f"problems on: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


def cmd_run_day(args, s: Settings) -> int:
    for step in (cmd_unzip, cmd_silver, cmd_check):
        rc = step(args, s)
        if rc:
            return rc
    args.start, args.end = args.date, args.date
    return cmd_gold(args, s)


def cmd_synth(args, s: Settings) -> int:
    template = Path(args.template)
    minutes = synthetic.hourly_minutes(args.per_hour)
    inc = (args.incident_detector, args.incident_from, args.incident_to) if args.incident_detector else None
    out = archive.day_dir(s.raw, "raw", args.date)
    n = synthetic.make_files(template, out, args.date, minutes, incident=inc)
    print(f"wrote {n} SYNTHETIC files to {out}")
    return 0


def cmd_benchmark(args, s: Settings) -> int:
    from . import benchmark

    _jprint(benchmark.run(s, args.date, args.repeat))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hktraffic", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def add(name, func, help_, date_arg=True):
        sp = sub.add_parser(name, help=help_)
        if date_arg:
            sp.add_argument("--date", type=_day, required=True, help="YYYY-MM-DD")
        sp.set_defaults(func=func)
        return sp

    add("unzip", cmd_unzip, "extract the day's zips from data/landing into data/raw")
    add("scan", cmd_scan, "list the zips found in data/landing and what they are", date_arg=False)
    sp = add("dims", cmd_dims, "build dim_detector (default: newest detector zip in data/landing)", date_arg=False)
    sp.add_argument("--detectors")
    add("silver", cmd_silver, "raw XML -> silver Parquet (with quality flags)")
    add("check", cmd_check, "quality gate for one day")
    sp = sub.add_parser("gold", help="silver -> gold tables (detector_hour, hotspots, anomalies)")
    sp.add_argument("--start", type=_day, required=True)
    sp.add_argument("--end", type=_day)
    sp.set_defaults(func=cmd_gold)
    add("serve", cmd_serve, "load gold + dimensions into PostgreSQL", date_arg=False)
    sp = add("export-bi", cmd_export_bi, "write the Power BI views to CSV files", date_arg=False)
    sp.add_argument("--out", default="data/bi")
    sp = sub.add_parser("run-range", help="several days in one Spark session: unzip, silver, check, then gold")
    sp.add_argument("--start", type=_day, required=True)
    sp.add_argument("--end", type=_day, required=True)
    sp.add_argument("--delete-raw", action="store_true", help="delete extracted XML after silver is written")
    sp.set_defaults(func=cmd_run_range)
    add("run-day", cmd_run_day, "unzip -> silver -> check -> gold for one day")
    sp = add("synth", cmd_synth, "write SYNTHETIC raw files (for tests and benchmarks only)")
    sp.add_argument("--template", default="tests/fixtures/real/rawSpeedVol-all.xml")
    sp.add_argument("--per-hour", type=int, default=1)
    sp.add_argument("--incident-detector")
    sp.add_argument("--incident-from", type=int, default=8)
    sp.add_argument("--incident-to", type=int, default=10)
    sp = add("benchmark", cmd_benchmark, "single-process Python vs Spark on the raw files of a day")
    sp.add_argument("--repeat", type=int, default=1)
    return p


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    return args.func(args, get_settings()) or 0


if __name__ == "__main__":
    sys.exit(main())
