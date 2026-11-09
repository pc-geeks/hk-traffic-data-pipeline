"""PostgreSQL integration tests (skipped when no database is reachable). No Spark needed."""
from pathlib import Path

import pytest

from hktraffic import serve

SQL = Path(__file__).resolve().parents[1] / "sql" / "analytics.sql"


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def detector_rows():
    return [
        {"detector_id": "AID1", "district": "Wan Chai", "road_en": "Gloucester Road", "corridor": "Gloucester Road",
         "location": "Wan Chai", "travel_direction": "Eastbound", "latitude": 22.28, "longitude": 114.17,
         "direction": "East", "rotation": 90},
        {"detector_id": "AID2", "district": "Kwun Tong", "road_en": "Kwun Tong Road", "corridor": "Kwun Tong Road",
         "location": None, "travel_direction": "Westbound", "latitude": 22.31, "longitude": 114.22,
         "direction": "West", "rotation": 270},
    ]


def hour_rows():
    rows = []
    # AID9 is deliberately missing from the dimension
    for det, speeds in (("AID1", [60, 25, 55]), ("AID2", [70, 65, 68]), ("AID9", [50, 50, 50])):
        for hour, sp in zip((7, 8, 9), speeds):
            rows.append({"detector_id": det, "obs_date": "2026-10-26", "hour": hour, "periods_seen": 100,
                         "lane_rows": 400, "usable_rows": 380, "volume": 900.0, "avg_occupancy": 12.0,
                         "congested_rows": 100 if sp < 30 else 0, "wavg_speed": float(sp),
                         "congested_share": 0.26 if sp < 30 else 0.0, "coverage": 0.83, "low_coverage": False,
                         "iso_dow": 1, "day_type": "weekday"})
    return rows


def test_upsert_is_idempotent_and_updates(pg, settings):
    assert serve.upsert(pg, settings.schema, "dim_detector", detector_rows()) == 2
    rows = detector_rows()
    rows[0]["district"] = "Eastern"
    serve.upsert(pg, settings.schema, "dim_detector", rows)
    got = q(pg, f"SELECT detector_id, district FROM {settings.schema}.dim_detector ORDER BY 1")
    assert got == [("AID1", "Eastern"), ("AID2", "Kwun Tong")]


def test_upsert_rejects_unknown_tables_and_bad_schema(pg, settings):
    with pytest.raises(ValueError):
        serve.upsert(pg, settings.schema, "pg_user; DROP TABLE x", [{"a": 1}])
    with pytest.raises(ValueError):
        serve.upsert(pg, "bad schema!", "dim_detector", detector_rows())
    assert serve.upsert(pg, settings.schema, "dim_detector", []) == 0


def test_record_run_upserts(pg, settings):
    m = {"day": "2026-10-28", "kind": "raw", "files": 1035, "rows": 4_300_000,
         "flags": {"ok": 4_000_000, "invalid_flag": 1000}, "detectors_reporting": 760}
    serve.record_run(pg, settings.schema, m)
    serve.record_run(pg, settings.schema, {**m, "files": 1036})
    rows = q(pg, f"SELECT files, flags->>'invalid_flag' FROM {settings.schema}.pipeline_run")
    assert rows == [(1036, "1000")]


def test_analytics_queries_run_and_keep_unknown_detectors(pg, settings):
    serve.upsert(pg, settings.schema, "dim_detector", detector_rows())
    serve.upsert(pg, settings.schema, "fact_detector_hour", hour_rows())
    serve.upsert(pg, settings.schema, "hotspot", [
        {"detector_id": "AID1", "mean_congested_share": 0.09, "mean_speed_kmh": 46.0, "days_observed": 1,
         "hours_observed": 3, "peak_hour": 8, "peak_hour_share": 0.26, "rank": 1},
        {"detector_id": "AID9", "mean_congested_share": 0.0, "mean_speed_kmh": 50.0, "days_observed": 1,
         "hours_observed": 3, "peak_hour": 7, "peak_hour_share": 0.0, "rank": 2}])
    serve.upsert(pg, settings.schema, "anomaly_hour", [
        {"detector_id": "AID1", "obs_date": "2026-10-26", "hour": 8, "wavg_speed": 25.0, "baseline_mean": 55.0,
         "baseline_std": 4.0, "baseline_days": 5, "z_score": -7.5, "is_anomaly": True, "volume": 900.0,
         "congested_share": 0.26}])
    serve.record_run(pg, settings.schema, {"day": "2026-10-26", "kind": "raw", "files": 3, "rows": 100,
                                           "flags": {"ok": 100}, "detectors_reporting": 3})
    text = SQL.read_text(encoding="utf-8").replace("hkt.", f"{settings.schema}.")
    statements = [s for s in text.split(";")
                  if any(ln.strip() and not ln.strip().startswith("--") for ln in s.splitlines())]
    assert len(statements) == 8
    results = [q(pg, s) for s in statements]
    hotspots = results[0]
    assert hotspots[0][1] == "AID1" and hotspots[0][2] == "Gloucester Road"
    assert hotspots[1][2] == "(unknown detector)"    # LEFT JOIN keeps detectors missing from the dimension
    assert results[4][0][2] == "AID1"                                 # anomaly listing
    assert results[6] == [("AID9", 3)]                               # the "missing from dimension" report


def _hour(det, hour, speed, volume, congested, usable, bad=0):
    return {"detector_id": det, "obs_date": "2026-10-26", "hour": hour, "periods_seen": 100, "lane_rows": usable + bad,
            "usable_rows": usable, "bad_rows": bad, "volume": float(volume), "avg_occupancy": 10.0,
            "congested_rows": congested, "wavg_speed": float(speed), "congested_share": congested / usable,
            "coverage": 0.8, "low_coverage": False, "iso_dow": 1, "day_type": "weekday"}


def _seed_corridor(pg, settings):
    third = {"detector_id": "AID3", "district": "Wan Chai", "road_en": "Gloucester Road near X - Eastbound",
             "corridor": "Gloucester Road", "location": "X", "travel_direction": "Eastbound",
             "latitude": 22.29, "longitude": 114.18, "direction": "East", "rotation": 90}
    dims = detector_rows() + [third]
    serve.upsert(pg, settings.schema, "dim_detector", dims)
    serve.upsert(pg, settings.schema, "fact_detector_hour", [
        _hour("AID1", 8, 20, 100, 30, 100, bad=10),
        _hour("AID3", 8, 60, 300, 0, 300),
        _hour("AID9", 8, 40, 50, 0, 50, bad=50)])      # AID9 is not in the dimension


def test_bi_corridor_view_weights_speed_by_volume(pg, settings):
    _seed_corridor(pg, settings)
    rows = q(pg, f"SELECT corridor, travel_direction, hour, detectors, wavg_speed, volume, congested_share "
                 f"FROM {settings.schema}.bi_corridor_hour ORDER BY corridor")
    gloucester = next(r for r in rows if r[0] == "Gloucester Road")
    assert gloucester[1:4] == ("Eastbound", 8, 2)
    assert gloucester[4] == pytest.approx((20 * 100 + 60 * 300) / 400)      # volume-weighted, not (20+60)/2
    assert gloucester[5] == 400 and gloucester[6] == pytest.approx(30 / 400)
    assert any(r[0] == "(unknown detector)" for r in rows)                  # LEFT JOIN keeps unknown detectors


def test_bi_detector_quality_and_coverage_views(pg, settings):
    _seed_corridor(pg, settings)
    qual = {r[0]: r for r in q(pg, f"SELECT detector_id, lane_rows, bad_rows, bad_share "
                                   f"FROM {settings.schema}.bi_detector_quality")}
    assert qual["AID9"][3] == pytest.approx(0.5) and qual["AID3"][3] == 0
    cov = q(pg, f"SELECT hour, detectors_reporting, avg_coverage FROM {settings.schema}.bi_coverage_hour")
    assert cov == [(8, 3, pytest.approx(0.8))]


def test_bi_views_have_readable_weekday_and_pipeline_flags(pg, settings):
    _seed_corridor(pg, settings)
    assert q(pg, f"SELECT DISTINCT weekday FROM {settings.schema}.bi_detector_hour") == [("Mon",)]
    serve.record_run(pg, settings.schema, {"day": "2026-10-26", "kind": "raw", "files": 3, "rows": 100,
                                           "flags": {"ok": 90, "invalid_flag": 4}, "detectors_reporting": 3})
    run = q(pg, f"SELECT ok_rows, invalid_rows, out_of_range_rows FROM {settings.schema}.bi_pipeline_run")
    assert run == [(90, 4, 0)]


def test_export_views_writes_csv_with_bom(pg, settings, tmp_path):
    _seed_corridor(pg, settings)
    counts = serve.export_views(pg, settings.schema, tmp_path)
    assert set(counts) == set(serve.BI_VIEWS) and counts["bi_detector_hour"] == 3
    raw = (tmp_path / "bi_detector_hour.csv").read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")                                   # UTF-8 BOM
    header = raw.decode("utf-8-sig").splitlines()[0].split(",")
    assert {"detector_id", "corridor", "weekday", "wavg_speed", "congested_share"} <= set(header)
    assert len(raw.decode("utf-8-sig").splitlines()) == 4                     # header + 3 rows
    with pytest.raises(ValueError):
        serve.export_views(pg, settings.schema, tmp_path, views=("pg_user",))


def test_bi_flags_are_numeric_not_t_or_f(pg, settings):
    _seed_corridor(pg, settings)
    serve.upsert(pg, settings.schema, "anomaly_hour", [
        {"detector_id": "AID1", "obs_date": "2026-10-26", "hour": 8, "wavg_speed": 20.0, "baseline_mean": 50.0,
         "baseline_std": 4.0, "baseline_days": 5, "z_score": -7.5, "is_anomaly": True, "volume": 100.0,
         "congested_share": 0.3}])
    flag, drop = q(pg, f"SELECT is_anomaly, drop_share FROM {settings.schema}.bi_anomaly")[0]
    assert flag == 1 and drop == pytest.approx(0.6)
    assert q(pg, f"SELECT DISTINCT low_coverage FROM {settings.schema}.bi_detector_hour") == [(0,)]
