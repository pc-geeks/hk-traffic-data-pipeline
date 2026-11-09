"""Tests against the REAL files from the Transport Department (downloaded 2026-10-29)."""
import collections

from hktraffic import parsing as p


def test_raw_xml_real_counts(real):
    rows = p.parse_raw_xml(real["raw"].read_bytes(), "raw.xml")
    assert len(rows) == 4140
    assert all(r[-1] is None for r in rows)                      # no parse errors
    assert collections.Counter(r[11] for r in rows) == {"Y": 4118, "N": 22}
    assert {r[1] for r in rows} == {"2026-10-29"}
    assert sorted({r[2] for r in rows}) == ["11:19:00", "11:19:30"]
    assert len({r[4] for r in rows}) == 764


def test_raw_xml_quirks_of_the_real_file(real):
    rows = p.parse_raw_xml(real["raw"].read_bytes())
    assert max(r[7] for r in rows) == 237.0                      # extreme speed on a single vehicle
    assert sorted({r[5] for r in rows}) == ["East", "North", "North East", "North West",
                                           "South", "South East", "South West", "West"]  # no trailing spaces
    assert {"Fast Lane", "Slow Lane", "Middle Lane 1"} <= {r[6] for r in rows}
    assert any(r[4].startswith("TDS") for r in rows)             # ids are not only AIDxxxxx
    assert all(r[10] is not None for r in rows[:50])             # <s.d.> element is read


def test_detector_csv_real(real):
    rows, rejects = p.parse_detector_csv(real["det_csv"])
    assert len(rows) == 807 and rejects == []
    assert rows[0]["detector_id"] == "AID01101"                 # BOM removed, id column found
    assert all(r["road_en"] == r["road_en"].strip() for r in rows)
    assert all(22.0 < r["latitude"] < 22.7 for r in rows)


def test_detector_road_en_is_a_location_description_not_a_road_name(real):
    rows, _ = p.parse_detector_csv(real["det_csv"])
    assert all(r["corridor"] for r in rows)
    assert len({r["corridor"] for r in rows}) == 81
    top = collections.Counter(r["corridor"] for r in rows).most_common(3)
    assert top == [("Tuen Mun Road", 75), ("North Lantau Highway", 45), ("Yuen Long Highway", 36)]
    assert collections.Counter(r["travel_direction"] for r in rows) == {
        "Eastbound": 223, "Westbound": 215, "Northbound": 188, "Southbound": 172, None: 9}


def test_district_spelling_is_normalised(real):
    rows, _ = p.parse_detector_csv(real["det_csv"])
    districts = {r["district"] for r in rows}
    assert len(districts) == 18 and "Central and Western" in districts and "Central & Western" not in districts
    assert sum(1 for r in rows if r["district"] == "Central and Western") == 14   # 13 + 1 merged


def test_split_location_patterns():
    assert p.split_location("Lion Rock Tunnel Road near Kak Tin Village - Northbound (1)") == \
        ("Lion Rock Tunnel Road", "Kak Tin Village", "Northbound")
    assert p.split_location("Island Eastern Corridor near North Point Ferry Pier Eastbound") == \
        ("Island Eastern Corridor", "North Point Ferry Pier", "Eastbound")      # direction glued to the place
    assert p.split_location("Lung Cheung Road near Diamond Hill MTR Station") == \
        ("Lung Cheung Road", "Diamond Hill MTR Station", None)                  # no direction at all
    assert p.split_location("Fanling Bypass - Westbound (2)") == (None, "Fanling Bypass - Westbound", "Westbound")
    assert p.split_location(None) == (None, None, None)
    assert p.split_location("") == (None, None, None)


def test_rows_without_near_inherit_a_known_corridor():
    data = (b"AID_ID_Number,Latitude,Longitude,Road_EN\n"
            b"A,22.3,114.1,Lung Cheung Road near Diamond Hill - Westbound\n"
            b"B,22.3,114.1,Lung Cheung Road Beacon Heights - Westbound (1)\n"
            b"C,22.3,114.1,Fanling Bypass - Eastbound (2)\n")
    rows, _ = p.parse_detector_csv(data)
    got = {r["detector_id"]: (r["corridor"], r["location"]) for r in rows}
    assert got == {"A": ("Lung Cheung Road", "Diamond Hill"), "B": ("Lung Cheung Road", "Beacon Heights"),
                   "C": ("Fanling Bypass", None)}


def test_every_raw_detector_is_in_the_dimension_but_not_vice_versa(real):
    raw = {r[4] for r in p.parse_raw_xml(real["raw"].read_bytes())}
    dim = {r["detector_id"] for r in p.parse_detector_csv(real["det_csv"])[0]}
    assert raw <= dim and len(dim - raw) == 43


# ------------------------------------------------------------- failure handling
def test_bad_xml_becomes_one_flagged_row_not_an_exception():
    for data, reason in ((b"<nope", "xml_parse_error"), (b"<other/>", "unexpected_root"),
                         (b"<raw_speed_volume_list/>", "missing_date_or_periods")):
        rows = p.parse_raw_xml(data, "bad.xml")
        assert len(rows) == 1 and rows[0][0] == "bad.xml" and rows[0][-1].startswith(reason)


def test_normalise_direction():
    assert p.normalise_direction("North ") == "North"
    assert p.normalise_direction("  south   east ") == "South East"
    assert p.normalise_direction("6") == "South East"
    assert p.normalise_direction(None) is None
    assert p.normalise_direction("Upwards") == "Upwards"          # unknown values are kept, not guessed


def test_csv_header_drift_is_tolerated():
    det = b"Device_ID,Latitude,Longitude\nAID1,22.3,114.1\n"
    assert p.parse_detector_csv(det)[0][0]["detector_id"] == "AID1"


def test_detector_csv_rejects_bad_rows():
    data = b"AID_ID_Number,Latitude,Longitude\n,22.3,114.1\nAID2,not-a-number,114.1\nAID3,51.0,0.1\nAID4,22.3,114.1\n"
    rows, rejects = p.parse_detector_csv(data)
    assert [r["detector_id"] for r in rows] == ["AID4"]
    assert [r["reason"] for r in rejects] == ["missing_detector_id", "invalid_coordinates", "invalid_coordinates"]
