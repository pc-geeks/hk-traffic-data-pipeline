"""Spark integration tests (need PySpark + Java). Slow on small machines: Spark starts a JVM once per session.

Part 1 uses the REAL files. Part 2 uses SYNTHETIC multi-day data (see synthetic.py) because a baseline for
anomaly detection needs several days and downloading them is not appropriate for a unit test.
"""
import collections
import shutil
from datetime import date

import pytest

from hktraffic import archive, parsing, spark_jobs, synthetic
from hktraffic.config import Settings

pytestmark = pytest.mark.spark
REAL_DAY = date(2026, 10, 29)


def place(settings, kind, day, src, name):
    d = archive.day_dir(settings.raw, kind, day)
    d.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, d / name)


# ------------------------------------------------------------ part 1: real files
def test_silver_lanes_matches_independent_python_calculation(spark, settings, real):
    place(settings, "raw", REAL_DAY, real["raw"], "rawSpeedVol_1119.xml")
    m = spark_jobs.build_silver_lanes(spark, settings, REAL_DAY, min_partitions=1, output_files=1)
    assert m["rows"] == 4140 and m["files"] == 1 and m["detectors_reporting"] == 764

    expected = collections.Counter()
    for r in parsing.parse_raw_xml(real["raw"].read_bytes()):
        sp, oc, vo, valid = r[7], r[8], r[9], r[11]
        if valid != "Y":
            expected["invalid_flag"] += 1
        elif sp is None or oc is None or vo is None:
            expected["missing_value"] += 1
        elif sp < 0 or sp > 150 or oc < 0 or oc > 100 or vo < 0:
            expected["out_of_range"] += 1
        elif vo == 0 and oc >= 15:
            expected["standstill_candidate"] += 1
        elif vo == 0:
            expected["no_vehicles"] += 1
        else:
            expected["ok"] += 1
    assert m["flags"] == dict(expected)
    assert m["flags"]["out_of_range"] == 13 and m["flags"]["invalid_flag"] == 22


def test_silver_is_idempotent(spark, settings, real):
    place(settings, "raw", REAL_DAY, real["raw"], "rawSpeedVol_1119.xml")
    a = spark_jobs.build_silver_lanes(spark, settings, REAL_DAY, min_partitions=1, output_files=1)
    b = spark_jobs.build_silver_lanes(spark, settings, REAL_DAY, min_partitions=1, output_files=1)
    assert a["rows"] == b["rows"] == 4140                      # overwritten, not appended


def test_corrupt_file_is_flagged_not_fatal(spark, settings, real):
    place(settings, "raw", REAL_DAY, real["raw"], "good.xml")
    bad = archive.day_dir(settings.raw, "raw", REAL_DAY) / "bad.xml"
    bad.write_bytes(b"<raw_speed_volume_list><date>2026-10-29")      # truncated download
    m = spark_jobs.build_silver_lanes(spark, settings, REAL_DAY, min_partitions=1, output_files=1)
    assert m["files"] == 2 and m["flags"]["parse_error"] == 1 and m["rows"] == 4141


def test_missing_folder_raises(spark, settings):
    with pytest.raises(FileNotFoundError):
        spark_jobs.build_silver_lanes(spark, settings, date(2000, 1, 1))


def test_dimensions(spark, settings, real):
    out = spark_jobs.build_dimensions(spark, settings, real["det_csv"].read_bytes())
    assert out == {"detectors": 807, "detector_rejects": 0}
    dim = spark.read.parquet(str(settings.silver / "dim_detector"))
    assert dim.count() == 807
    assert {"corridor", "location", "travel_direction", "district"} <= set(dim.columns)
    assert dim.filter("corridor IS NULL").count() == 0
    assert dim.select("district").distinct().count() == 18


# --------------------------------------------------- part 2: synthetic multi-day
@pytest.fixture(scope="module")
def multi(spark, tmp_path_factory):
    """Mon 19 Oct .. Fri 23 Oct (baseline) + Mon 26 Oct (target, with an injected incident)."""
    s = Settings(data_dir=tmp_path_factory.mktemp("multi"), spark_shuffle_partitions=4, min_hour_coverage=0.0)
    template = parsing_template()
    rows = parsing.parse_raw_xml(template.read_bytes())
    incident_det = next(r[4] for r in rows
                        if not synthetic._is_hotspot(r[4]) and r[7] >= 50 and r[9] > 0 and r[11] == "Y")
    days = [date(2026, 10, d) for d in (19, 20, 21, 22, 23, 26)]
    for day in days:
        inc = (incident_det, 8, 10) if day == date(2026, 10, 26) else None
        synthetic.make_files(template, archive.day_dir(s.raw, "raw", day), day,
                             [(8, 0), (13, 0), (18, 0)], incident=inc)
        spark_jobs.build_silver_lanes(spark, s, day, min_partitions=2, output_files=1)
    dh = spark_jobs.build_detector_hour(spark, s, days)
    spark_jobs.write_gold(dh, s, "detector_hour", files=1)
    return {"s": s, "days": days, "incident": incident_det}


def parsing_template():
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / "real" / "rawSpeedVol-all.xml"


def test_detector_hour_basics(spark, multi):
    s = multi["s"]
    dh = spark.read.parquet(str(s.gold / "detector_hour"))
    rows = dh.filter("dt = '2026-10-19'").collect()
    assert {r["hour"] for r in rows} == {8, 13, 18}
    # one file = two 30-second periods, except the real quirk: AID09402 only appears in one of them
    assert all(r["periods_seen"] in (1, 2) for r in rows)
    assert sum(1 for r in rows if r["periods_seen"] == 2) > 0.99 * len(rows)
    assert all(r["coverage"] == pytest.approx(r["periods_seen"] / 120) for r in rows)
    assert all(r["day_type"] == "weekday" and r["iso_dow"] == 1 for r in rows)
    r = next(x for x in rows if x["wavg_speed"] is not None)
    assert 0 < r["wavg_speed"] < 150 and 0 <= r["congested_share"] <= 1


def test_low_coverage_flag_uses_the_threshold(spark, multi):
    strict = Settings(data_dir=multi["s"].data_dir, spark_shuffle_partitions=4, min_hour_coverage=0.5)
    dh = spark_jobs.build_detector_hour(spark, strict, [date(2026, 10, 19)])
    assert dh.filter("low_coverage").count() == dh.count() > 0           # 2 of 120 periods is far below 50%


def test_weighted_speed_ignores_flagged_rows(spark, multi):
    s = multi["s"]
    lanes = spark.read.parquet(str(s.silver / "lane_30s")).filter("dt = '2026-10-19' AND hour(obs_ts) = 8")
    det = lanes.filter("quality_flag = 'ok'").select("detector_id").first()["detector_id"]
    ok = [r for r in lanes.filter(f"detector_id = '{det}' AND quality_flag = 'ok'").collect()]
    expected = sum(r["speed"] * r["volume"] for r in ok) / sum(r["volume"] for r in ok)
    got = spark.read.parquet(str(s.gold / "detector_hour")).filter(
        f"dt = '2026-10-19' AND hour = 8 AND detector_id = '{det}'").first()["wavg_speed"]
    assert got == pytest.approx(expected)


def test_hotspots_find_the_synthetic_congested_detectors(spark, multi):
    s = multi["s"]
    hs = spark_jobs.build_hotspots(spark, s, multi["days"])
    top = [r["detector_id"] for r in hs.orderBy("rank").limit(20).collect()]
    hot = sum(1 for d in top if synthetic._is_hotspot(d))
    assert hot >= 12, f"only {hot} of the top 20 are synthetic hotspots"
    assert {r["peak_hour"] for r in hs.orderBy("rank").limit(10).collect()} <= {8, 18}


def test_anomaly_detector_finds_the_injected_incident(spark, multi):
    s = multi["s"]
    an = spark_jobs.build_anomalies(spark, s, date(2026, 10, 26), multi["days"])
    flagged = {(r["detector_id"], r["hour"]) for r in an.filter("is_anomaly").collect()}
    assert (multi["incident"], 8) in flagged
    assert len(flagged) <= 5, f"too many false positives: {len(flagged)}"
    assert an.filter("baseline_days < 4").count() == 0


def test_anomalies_need_enough_history(spark, multi):
    s = multi["s"]
    short_history = [date(2026, 10, 19), date(2026, 10, 20), date(2026, 10, 26)]
    an = spark_jobs.build_anomalies(spark, s, date(2026, 10, 26), short_history)
    assert an.count() == 0                                           # only 2 baseline days: report nothing, not noise


def test_bad_rows_in_gold_match_bad_flags_in_silver(spark, multi):
    from pyspark.sql import functions as F

    s = multi["s"]
    silver = spark.read.parquet(str(s.silver / "lane_30s")).filter("dt = '2026-10-19'")
    bad_silver = silver.filter(F.col("quality_flag").isin("invalid_flag", "out_of_range", "missing_value",
                                                        "parse_error")).count()
    gold = spark.read.parquet(str(s.gold / "detector_hour")).filter("dt = '2026-10-19'")
    assert gold.agg(F.sum("bad_rows")).first()[0] == bad_silver > 0
    assert gold.filter("bad_rows > lane_rows").count() == 0


def test_gold_uses_observation_date_across_the_archive_boundary(spark, tmp_path):
    """Real archives: the file labelled D holds data stamped 23:5x of D-1, so the tail of D is in D+1's archive."""
    s = Settings(data_dir=tmp_path / "bd", spark_shuffle_partitions=4, min_hour_coverage=0.0)
    tpl = parsing_template()
    d, prev, nxt = date(2026, 10, 27), date(2026, 10, 26), date(2026, 10, 28)
    # archive of D: a normal morning file + a file whose internal date is D-1 (23:55)
    synthetic.make_files(tpl, archive.day_dir(s.raw, "raw", d), d, [(9, 0)])
    synthetic.make_files(tpl, archive.day_dir(s.raw, "raw", d), prev, [(23, 55)])
    # archive of D+1: a file whose internal date is D (23:56) = the tail of day D
    synthetic.make_files(tpl, archive.day_dir(s.raw, "raw", nxt), nxt, [(9, 0)])
    synthetic.make_files(tpl, archive.day_dir(s.raw, "raw", nxt), d, [(23, 56)])
    for day in (d, nxt):
        spark_jobs.build_silver_lanes(spark, s, day, min_partitions=1, output_files=1)

    dh = spark_jobs.build_detector_hour(spark, s, [d, nxt])
    dates = {r["obs_date"].isoformat() for r in dh.select("obs_date").distinct().collect()}
    assert dates == {"2026-10-27", "2026-10-28"}                       # no stray 2026-10-26 partition
    d_hours = {r["hour"] for r in dh.filter("dt = '2026-10-27'").select("hour").distinct().collect()}
    assert d_hours == {9, 23}                                          # hour 23 comes from the NEXT archive day

    only_d = spark_jobs.build_detector_hour(spark, s, [d])             # last requested day: its tail is still found
    assert {r["hour"] for r in only_d.select("hour").distinct().collect()} == {9, 23}
    assert only_d.select("obs_date").distinct().count() == 1


def test_anomaly_output_has_the_columns_serving_needs(spark, multi):
    from hktraffic.serve import TABLES

    an = spark_jobs.build_anomalies(spark, multi["s"], date(2026, 10, 26), multi["days"])
    assert set(TABLES["anomaly_hour"][0]) <= set(an.columns)
    assert an.filter("obs_date IS NULL").count() == 0


def test_cli_run_range_end_to_end_with_hash_named_zips(spark, tmp_path, real, monkeypatch):
    """Regression for the two real-data problems: opaque zip names, and day boundaries in the archive."""
    import json
    import zipfile

    from hktraffic import cli

    data = tmp_path / "data"
    landing = data / "landing"
    landing.mkdir(parents=True)
    stage = tmp_path / "stage"
    tpl = parsing_template()
    for i, day in enumerate((date(2026, 10, 26), date(2026, 10, 27))):
        d = stage / day.isoformat()
        synthetic.make_files(tpl, d, day, [(9, 0)])
        synthetic.make_files(tpl, d, day.replace(day=day.day - 1), [(23, 55)])      # stamped D-1, inside archive D
        with zipfile.ZipFile(landing / f"{str(i) * 64}-{day:%Y%m%d}.zip", "w", zipfile.ZIP_DEFLATED) as z:
            for p in sorted(d.glob("*.xml")):
                z.write(p, f"https%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Ftraffic-detectors%2F/{p.name}")
        with zipfile.ZipFile(landing / f"{str(i + 5) * 64}-{day:%Y%m%d}.zip", "w", zipfile.ZIP_DEFLATED) as z:
            inner = f"https%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Ftraffic-detectors%2F/{day:%Y%m%d}-0002-irn.xml"
            z.write(real["seg"], inner)
    for k, v in {"HKT_DATA_DIR": str(data), "HKT_MIN_FILE_RATIO": "0.0001", "HKT_MIN_HOUR_COVERAGE": "0",
                 "HKT_SHUFFLE_PARTITIONS": "4"}.items():
        monkeypatch.setenv(k, v)

    rc = cli.main(["run-range", "--start", "2026-10-26", "--end", "2026-10-27", "--delete-raw"])
    assert rc == 0
    assert not list((data / "raw" / "raw").glob("dt=*"))                       # --delete-raw removed the XML
    parts = sorted(p.name for p in (data / "gold" / "detector_hour").glob("dt=*"))
    assert parts == ["dt=2026-10-26", "dt=2026-10-27"]                         # no stray 2026-10-25 partition
    metrics = json.loads((data / "silver" / "_metrics" / "raw_2026-10-26.json").read_text())
    assert metrics["files"] == 2 and metrics["rows"] == 2 * 4140
