import zipfile
from datetime import date

import pytest

from hktraffic import archive


def test_classify_zip_real_names():
    assert archive.classify_zip("Traffic Speed, Volume and Road Occupancy (Raw Data) - 20261028.zip") == \
        ("raw", date(2026, 10, 28))
    assert archive.classify_zip("Locations of Traffic Detectors - 202604.zip") is None   # monthly dimension file
    assert archive.classify_zip("Raw Data - 20261340.zip") is None                       # impossible date


def test_find_zips(tmp_path):
    for n in ("A (Raw Data) - 20261028.zip", "A (Raw Data) - 20261027.zip"):
        (tmp_path / n).write_bytes(b"")
    found = archive.find_zips(tmp_path, date(2026, 10, 28))
    assert [p.name for p in found["raw"]] == ["A (Raw Data) - 20261028.zip"]


def make_zip(path, members):
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return path


def test_unzip_is_idempotent_and_ignores_non_xml(tmp_path):
    z = make_zip(tmp_path / "x.zip", {"a.xml": "<a/>", "sub/b.xml": "<b/>", "readme.txt": "hi", "d/": ""})
    dest = tmp_path / "out"
    first = archive.unzip_day(z, dest)
    assert first["extracted"] == 2 and first["ignored"] == 2
    assert sorted(p.name for p in dest.iterdir()) == ["a.xml", "b.xml"]      # flattened
    second = archive.unzip_day(z, dest)
    assert second["extracted"] == 0 and second["skipped"] == 2


def test_unzip_blocks_path_traversal(tmp_path):
    z = make_zip(tmp_path / "evil.zip", {"../../evil.xml": "<x/>"})
    dest = tmp_path / "safe" / "out"
    archive.unzip_day(z, dest)
    assert (dest / "evil.xml").exists()
    assert not (tmp_path / "evil.xml").exists()


def test_read_csv_bytes_from_zip_and_plain(tmp_path):
    (tmp_path / "d.csv").write_bytes(b"a,b\n1,2\n")
    assert archive.read_csv_bytes(tmp_path / "d.csv") == b"a,b\n1,2\n"
    z = make_zip(tmp_path / "d.zip", {"inner.csv": "a,b\n3,4\n"})
    assert archive.read_csv_bytes(z) == b"a,b\n3,4\n"
    with pytest.raises(ValueError):
        archive.read_csv_bytes(make_zip(tmp_path / "e.zip", {"x.txt": "no csv"}))


HASH = "7c383a97aae0e6e2b1a34a1d5f3e9c8b0a6d4e2f1c3b5a79e8d6c4b2a1f0e9d8"   # real packages are named <hash>-<date>.zip


def test_hash_named_zips_are_classified_by_content(tmp_path, real):
    raw = make_zip(tmp_path / f"{HASH}-20261026.zip", {
        "https%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Ftraffic-detectors%2F/20261026-0004-rawSpeedVol-all.xml":
            real["raw"].read_bytes()[:3000].decode("utf-8", "ignore") + "</x>",
        "https%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Ftraffic-detectors%2F/20221214-0000-data-dictionary.pdf": "x"})
    other = make_zip(tmp_path / f"{'a' * 64}-20261026.zip", {
        "folder/20261026-0002-other.xml": "<other_list></other_list>"})
    det = make_zip(tmp_path / f"{'b' * 64}-202604.zip", {"20260430-1842-traffic_speed_volume_occ_info.csv":
                                                         real["det_csv"].read_bytes().decode("utf-8-sig")})
    junk = make_zip(tmp_path / f"{'d' * 64}-20261026.zip", {"readme.txt": "nothing useful"})

    assert archive.peek_kind(raw) == "raw"
    assert archive.peek_kind(det) == "detectors"
    assert archive.peek_kind(other) is None and archive.peek_kind(junk) is None
    assert archive.classify_path(raw) == ("raw", date(2026, 10, 26))
    assert archive.classify_path(det) is None                       # monthly dimension package: not a day

    found = archive.find_zips(tmp_path, date(2026, 10, 26))
    assert [p.name for p in found["raw"]] == [raw.name]
    scan = archive.scan_landing(tmp_path)
    assert list(scan["days"]) == [date(2026, 10, 26)]
    assert [p.name for p in scan["detectors"]] == [det.name]
    assert [p.name for p in scan["unknown"]] == [other.name, junk.name]


def test_dimension_zips_are_ordered_by_month(tmp_path, real):
    csv = real["det_csv"].read_bytes().decode("utf-8-sig")
    new = make_zip(tmp_path / f"{'e' * 64}-202604.zip", {"a.csv": csv})
    old = make_zip(tmp_path / f"{'f' * 64}-202301.zip", {"a.csv": csv})
    assert [p.name for p in archive.scan_landing(tmp_path)["detectors"]] == [old.name, new.name]   # newest last


def test_unzip_flattens_the_url_named_folder(tmp_path):
    folder = "https%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Ftraffic-detectors%2F"
    z = make_zip(tmp_path / "x.zip", {f"{folder}/": "", f"{folder}/20261026-0004-rawSpeedVol-all.xml": "<a/>"})
    out = archive.unzip_day(z, tmp_path / "out")
    assert out["extracted"] == 1 and [p.name for p in (tmp_path / "out").iterdir()] == [
        "20261026-0004-rawSpeedVol-all.xml"]
