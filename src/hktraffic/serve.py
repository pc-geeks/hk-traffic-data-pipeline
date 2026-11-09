"""Serving layer: load small gold tables into PostgreSQL (idempotent upserts) for SQL analytics / BI tools.

Only tables and columns listed in TABLES can be written, because table names are interpolated into SQL.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

SQL_DIR = Path(__file__).parent / "sql"
_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

TABLES = {
    "dim_detector": (("detector_id", "district", "road_en", "corridor", "location", "travel_direction",
                      "road_tc", "road_sc", "latitude", "longitude", "direction", "rotation"), ("detector_id",)),
    "fact_detector_hour": (("detector_id", "obs_date", "hour", "periods_seen", "lane_rows", "usable_rows",
                            "bad_rows", "volume", "avg_occupancy", "congested_rows", "wavg_speed", "congested_share",
                            "coverage", "low_coverage", "iso_dow", "day_type"),
                           ("detector_id", "obs_date", "hour")),
    "hotspot": (("detector_id", "mean_congested_share", "mean_speed_kmh", "days_observed", "hours_observed",
                 "peak_hour", "peak_hour_share", "rank"), ("detector_id",)),
    "anomaly_hour": (("detector_id", "obs_date", "hour", "wavg_speed", "baseline_mean", "baseline_std",
                      "baseline_days", "z_score", "is_anomaly", "volume", "congested_share"),
                     ("detector_id", "obs_date", "hour")),
}


BI_VIEWS = ("bi_detector_hour", "bi_corridor_hour", "bi_hotspot", "bi_anomaly",
            "bi_coverage_hour", "bi_detector_quality", "bi_pipeline_run")


def connect(dsn: str):
    return psycopg2.connect(dsn)


def _schema(schema: str) -> str:
    if not _SCHEMA_RE.match(schema):
        raise ValueError(f"invalid schema name: {schema!r}")
    return schema


def init_schema(conn, schema: str) -> None:
    text = (SQL_DIR / "serve_schema.sql").read_text(encoding="utf-8").replace("__SCHEMA__", _schema(schema))
    with conn, conn.cursor() as cur:
        cur.execute(text)


def upsert(conn, schema: str, table: str, rows: list) -> int:
    """Upsert dict rows into an allow-listed table. Safe to re-run (idempotent)."""
    if table not in TABLES:
        raise ValueError(f"table not allowed: {table!r}")
    schema = _schema(schema)
    columns, keys = TABLES[table]
    if not rows:
        return 0
    values = [tuple(r.get(c) for c in columns) for r in rows]
    updates = [c for c in columns if c not in keys]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in updates)
    sql = (f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES %s "
           f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {set_clause}")
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=2000)
    return len(values)


def record_run(conn, schema: str, metrics: dict) -> None:
    schema = _schema(schema)
    with conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {schema}.pipeline_run (day, kind, files, rows, flags, detectors_reporting) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (day, kind) DO UPDATE SET files = EXCLUDED.files, "
            "rows = EXCLUDED.rows, flags = EXCLUDED.flags, detectors_reporting = EXCLUDED.detectors_reporting, "
            "loaded_at = now()",
            (metrics["day"], metrics["kind"], metrics["files"], metrics["rows"],
             Json(metrics["flags"], dumps=json.dumps), metrics.get("detectors_reporting")))


def export_views(conn, schema: str, out_dir, views=BI_VIEWS) -> dict:
    """Write each BI view to <out_dir>/<view>.csv (UTF-8 with BOM, so Excel and Power BI read names correctly).
    Lets people open the data in Power BI without installing a PostgreSQL connector."""
    schema = _schema(schema)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    counts = {}
    for view in views:
        if view not in BI_VIEWS:
            raise ValueError(f"not a BI view: {view!r}")
        path = out / f"{view}.csv"
        with conn.cursor() as cur, open(path, "w", encoding="utf-8-sig", newline="") as f:
            cur.copy_expert(f"COPY (SELECT * FROM {schema}.{view}) TO STDOUT WITH (FORMAT csv, HEADER true)", f)
            counts[view] = max(cur.rowcount, 0)
    return counts
