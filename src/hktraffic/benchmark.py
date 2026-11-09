"""Honest comparison: single-process Python vs Spark on the same raw files, computing the same result
(per detector-hour volume-weighted speed). Run it on YOUR machine with as many files as you have: the
interesting result is the data size at which Spark starts to pay off, and it depends on the CPU cores.

On a 1-core machine Spark will usually LOSE (startup + scheduling overhead with no parallelism to gain).
That is a legitimate finding, not a bug: report it.
"""
from __future__ import annotations

import time
from collections import defaultdict
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from . import parsing, spark_jobs
from .config import Settings


def python_baseline(folder: Path) -> dict:
    """Parse every file and aggregate with plain dicts. One process, no framework."""
    acc = defaultdict(lambda: [0.0, 0.0])
    rows = 0
    for path in sorted(folder.glob("*.xml")):
        for r in parsing.parse_raw_xml(path.read_bytes(), path.name):
            rows += 1
            _, obs_date, p_from, _, det, _, _, speed, occ, vol, _, valid, err = r
            if err or valid != "Y" or speed is None or vol is None or vol <= 0:
                continue
            if speed < 0 or speed > 150 or occ is None or occ < 0 or occ > 100:
                continue
            key = (obs_date, int(p_from[:2]), det)
            acc[key][0] += speed * vol
            acc[key][1] += vol
    return {"rows": rows, "groups": len(acc)}


def spark_run(settings: Settings, day: date) -> dict:
    spark = spark_jobs.get_spark(settings, "hktraffic-benchmark")
    try:
        folder = settings.raw / "raw" / f"dt={day:%Y-%m-%d}"
        df = spark_jobs._xml_rows(spark, folder, parsing.parse_raw_xml, spark_jobs.RAW_SCHEMA,
                                  min_partitions=max(2, spark.sparkContext.defaultParallelism))
        ok = (F.col("valid") == "Y") & F.col("speed").between(0, 150) & F.col("occupancy").between(0, 100) \
            & (F.col("volume") > 0) & F.col("parse_error").isNull()
        g = (df.filter(ok).withColumn("hour", F.substring("period_from", 1, 2).cast("int"))
             .groupBy("obs_date", "hour", "detector_id")
             .agg((F.sum(F.col("speed") * F.col("volume")) / F.sum("volume")).alias("wavg")))
        return {"groups": g.count(), "rows": df.count(), "cores": spark.sparkContext.defaultParallelism}
    finally:
        spark.stop()


def run(settings: Settings, day: date, repeat: int = 1) -> dict:
    folder = settings.raw / "raw" / f"dt={day:%Y-%m-%d}"
    files = sum(1 for _ in folder.glob("*.xml"))
    size_mb = sum(p.stat().st_size for p in folder.glob("*.xml")) / 1e6
    result = {"files": files, "xml_mb": round(size_mb, 1), "runs": []}
    for _ in range(repeat):
        t = time.perf_counter()
        py = python_baseline(folder)
        t_py = time.perf_counter() - t
        t = time.perf_counter()
        sp = spark_run(settings, day)
        t_sp = time.perf_counter() - t
        result["runs"].append({
            "python_seconds": round(t_py, 1), "spark_seconds": round(t_sp, 1),
            "python_rows": py["rows"], "spark_rows": sp["rows"],
            "same_groups": py["groups"] == sp["groups"], "spark_cores": sp["cores"],
            "speedup": round(t_py / t_sp, 2) if t_sp else None,
        })
    result["note"] = "Spark time includes JVM startup. Run with more files and more cores to see the trend."
    return result
