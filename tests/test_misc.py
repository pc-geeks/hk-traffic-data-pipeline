import pytest
import requests

from hktraffic import archive_api, quality, synthetic
from hktraffic.cli import build_parser
from hktraffic.config import Settings


# ------------------------------------------------------------------- quality gate
def metrics(**kw):
    m = {"kind": "raw", "files": 1035, "rows": 4_000_000,
         "flags": {"ok": 3_900_000, "invalid_flag": 2000, "out_of_range": 500, "no_vehicles": 97_500},
         "detectors_reporting": 760, "detectors_known": 807}
    m.update(kw)
    return m


def test_real_archive_day_passes_with_default_thresholds():
    # 1035 of 1440 files (72%) is what the real 2026-10-28 archive contained
    assert quality.evaluate(metrics(), Settings()) == []


def test_missing_files_parse_errors_and_flag_share_fail():
    assert "files" in quality.evaluate(metrics(files=300), Settings())[0]
    parse_failures = quality.evaluate(metrics(flags={"ok": 10, "parse_error": 2}), Settings())
    assert any("failed to parse" in f for f in parse_failures)
    f = quality.evaluate(metrics(flags={"ok": 100, "invalid_flag": 50}, rows=150), Settings())
    assert any("flagged bad" in x for x in f)


def test_no_data_and_few_detectors():
    assert quality.evaluate(metrics(files=0, rows=0), Settings()) == ["raw: no data"]
    assert any("known detectors" in f for f in quality.evaluate(metrics(detectors_reporting=100), Settings()))


def test_no_vehicles_is_normal_not_bad():
    m = metrics(flags={"ok": 10, "no_vehicles": 990}, rows=1000)
    assert quality.evaluate(m, Settings()) == []


def test_check_raises():
    with pytest.raises(quality.DataQualityError):
        quality.check(metrics(files=1), Settings())


# --------------------------------------------------------------------- archive api
class Resp:
    def __init__(self, status=200, body=None, content=b""):
        self.status_code, self._b, self.content = status, body, content

    def json(self):
        return self._b

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


def test_archive_api_calls_and_summary(monkeypatch):
    calls = []

    def fake(url, params=None, timeout=None):
        calls.append((url, params))
        return Resp(body={"version-count": 2, "total-size": 1234, "timestamps": ["20261028-0000", "20261028-0001"]})

    monkeypatch.setattr(archive_api.requests, "get", fake)
    payload = archive_api.list_file_versions("https://x/y.xml", "20261028", "20261028")
    assert calls[0][0].endswith("/list-file-versions") and calls[0][1]["start"] == "20261028"
    assert archive_api.summarise_versions(payload) == {
        "version_count": 2, "total_size_bytes": 1234, "first": "20261028-0000", "last": "20261028-0001"}
    with pytest.raises(ValueError):
        archive_api.summarise_versions([])


def test_archive_api_does_not_retry_404(monkeypatch):
    n = []
    monkeypatch.setattr(archive_api.requests, "get", lambda *a, **k: n.append(1) or Resp(404))
    with pytest.raises(requests.HTTPError):
        archive_api.get_file("u", "20261028-0000")
    assert len(n) == 1


# ------------------------------------------------------------------- synthetic data
def test_synthetic_files_are_valid_and_deterministic(tmp_path, real):
    from datetime import date

    from hktraffic import parsing

    a = synthetic.make_files(real["raw"], tmp_path / "a", date(2026, 10, 26), [(8, 0)], seed=1)
    b = synthetic.make_files(real["raw"], tmp_path / "b", date(2026, 10, 26), [(8, 0)], seed=1)
    fa = (tmp_path / "a" / "rawSpeedVol-20261026-0800.xml").read_bytes()
    fb = (tmp_path / "b" / "rawSpeedVol-20261026-0800.xml").read_bytes()
    assert a == b == 1 and fa == fb
    rows = parsing.parse_raw_xml(fa)
    assert len(rows) == 4140 and rows[0][1] == "2026-10-26" and rows[0][2] == "08:00:00"


def test_synthetic_rush_hour_hotspots_are_slower(tmp_path, real):
    from datetime import date

    from hktraffic import parsing

    out = tmp_path / "s"
    synthetic.make_files(real["raw"], out, date(2026, 10, 26), [(8, 0), (13, 0)], seed=3)   # Monday
    hot = [r for r in parsing.parse_raw_xml((out / "rawSpeedVol-20261026-0800.xml").read_bytes())
           if synthetic._is_hotspot(r[4]) and r[11] == "Y" and r[7] < 150]
    calm = [r for r in parsing.parse_raw_xml((out / "rawSpeedVol-20261026-1300.xml").read_bytes())
            if synthetic._is_hotspot(r[4]) and r[11] == "Y" and r[7] < 150]
    assert sum(r[7] for r in hot) / len(hot) < 0.6 * sum(r[7] for r in calm) / len(calm)


# ------------------------------------------------------------------------- cli
def test_parser():
    p = build_parser()
    assert p.parse_args(["unzip", "--date", "2026-10-28"]).date.isoformat() == "2026-10-28"
    a = p.parse_args(["gold", "--start", "2026-10-20", "--end", "2026-10-26"])
    assert a.end.isoformat() == "2026-10-26"
    assert p.parse_args(["dims", "--detectors", "a.csv"]).detectors == "a.csv"
    with pytest.raises(SystemExit):
        p.parse_args(["silver"])          # --date is required
    with pytest.raises(SystemExit):
        p.parse_args(["unzip", "--date", "28/10/2026"])


def test_schema_name_validated():
    with pytest.raises(ValueError):
        Settings(schema="x; DROP TABLE y")


def test_metrics_files_are_saved_and_loadable(tmp_path):
    import json

    from hktraffic import cli

    s = Settings(data_dir=tmp_path)
    cli._save_metrics(s, [{"kind": "raw", "day": "2026-10-26", "files": 3, "rows": 12420,
                           "flags": {"ok": 11000}, "detectors_reporting": 764}])
    saved = json.loads((tmp_path / "silver" / "_metrics" / "raw_2026-10-26.json").read_text())
    assert saved["files"] == 3 and saved["flags"] == {"ok": 11000}
