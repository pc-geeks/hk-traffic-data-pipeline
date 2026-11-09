"""Pure parsing functions (no Spark, no database), written against the REAL files, not only the spec.

Differences between the official data specification (Nov 2022) and the real files (checked Oct 2026):
  * the lane standard-deviation element is `<s.d.>`, not `sd`
  * direction is text ("South East"), not a code, and 12 values carry trailing spaces ("North ")
  * lane ids are "Fast Lane" / "Middle Lane" / "Slow Lane" and also "Middle Lane 1".."Middle Lane 4"
  * detector ids are not only `AIDxxxxx` (also `TDS90018`, `TDSCPRPS10003`, ...)
  * the detector CSV has a UTF-8 BOM and the id column is `AID_ID_Number` (spec: `Device_ID`)

Parsers never raise on bad content: a file that cannot be parsed yields ONE row whose `parse_error`
is set, so the problem is visible downstream instead of silently shrinking the data.
"""
from __future__ import annotations

import csv
import io
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Optional

RAW_COLUMNS = (
    "source_file", "obs_date", "period_from", "period_to", "detector_id", "direction",
    "lane_id", "speed", "occupancy", "volume", "sd", "valid", "parse_error",
)

DIRECTION_CODES = {
    "1": "North", "2": "East", "3": "South", "4": "West",
    "5": "North East", "6": "South East", "7": "North West", "8": "South West",
}
DIRECTIONS = frozenset(DIRECTION_CODES.values())


def _text(node: Optional[ET.Element], tag: str) -> Optional[str]:
    if node is None:
        return None
    child = node.find(tag)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def _num(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except ValueError:
        return None
    return f if f == f and f not in (float("inf"), float("-inf")) else None


def normalise_direction(value: Optional[str]) -> Optional[str]:
    """Trim, fix case, and accept the spec's numeric codes. Unknown values are returned trimmed."""
    if value is None:
        return None
    v = " ".join(value.split())
    if v in DIRECTION_CODES:
        return DIRECTION_CODES[v]
    titled = v.title()
    return titled if titled in DIRECTIONS else v or None


def _error_row(columns: tuple, source_file: str, message: str) -> tuple:
    row = {c: None for c in columns}
    row["source_file"] = source_file
    row["parse_error"] = message[:300]
    return tuple(row[c] for c in columns)


# ----------------------------------------------------------------- raw detector XML
def parse_raw_xml(data: bytes, source_file: str = "") -> list:
    """`rawSpeedVol-all.xml` -> one tuple (see RAW_COLUMNS) per detector lane per 30-second period."""
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        return [_error_row(RAW_COLUMNS, source_file, f"xml_parse_error: {exc}")]
    if root.tag != "raw_speed_volume_list":
        return [_error_row(RAW_COLUMNS, source_file, f"unexpected_root: {root.tag}")]
    obs_date = _text(root, "date")
    periods = root.find("periods")
    if obs_date is None or periods is None:
        return [_error_row(RAW_COLUMNS, source_file, "missing_date_or_periods")]

    rows = []
    for period in periods.findall("period"):
        p_from, p_to = _text(period, "period_from"), _text(period, "period_to")
        detectors = period.find("detectors")
        for det in (detectors.findall("detector") if detectors is not None else []):
            det_id = _text(det, "detector_id")
            direction = normalise_direction(_text(det, "direction"))
            lanes = det.find("lanes")
            for lane in (lanes.findall("lane") if lanes is not None else []):
                rows.append((
                    source_file, obs_date, p_from, p_to, det_id, direction,
                    _text(lane, "lane_id"),
                    _num(_text(lane, "speed")), _num(_text(lane, "occupancy")),
                    _num(_text(lane, "volume")), _num(_text(lane, "s.d.")),
                    (_text(lane, "valid") or "").upper() or None,
                    None,
                ))
    return rows or [_error_row(RAW_COLUMNS, source_file, "no_lane_rows")]


# ------------------------------------------------------------- location text helpers
_NEAR = re.compile(r"^(?P<corridor>.*?)\s+near\s+(?P<location>.*)$", re.IGNORECASE)
_BOUND = re.compile(r"\b(North|South|East|West)bound\b", re.IGNORECASE)
_TRAILING_N = re.compile(r"\s*\(\d+\)\s*$")


def normalise_district(value: Optional[str]) -> Optional[str]:
    """The real file spells one district two ways ("Central & Western" 13x, "Central and Western" 1x)."""
    if not value:
        return None
    return " ".join(value.replace("&", "and").split())


def split_location(road_en: Optional[str]) -> tuple:
    """'Lion Rock Tunnel Road near Toll Plaza - Northbound (1)' -> (corridor, location, travel_direction).

    In the real detector file `Road_EN` is NOT a road name but a location description:
    "<corridor> near <place> - <direction>" (767 of 807 rows), and 40 rows deviate from that pattern
    (no "near", no dash, direction glued to the place). Best effort, never raises:
      * corridor = text before " near "
      * travel_direction = "Northbound" etc. when a *bound word is present
    Rows without " near " are completed by `enrich_corridors` using the corridors seen elsewhere.
    """
    if not road_en:
        return None, None, None
    text = _TRAILING_N.sub("", road_en.strip())
    direction = None
    m = _BOUND.search(text)
    if m:
        direction = m.group(1).title() + "bound"
    near = _NEAR.match(text)
    if near:
        loc = near.group("location")
        loc = re.sub(r"\s*-\s*[A-Za-z ]*bound\s*$", "", loc, flags=re.IGNORECASE)
        loc = _BOUND.sub("", loc).strip(" -")
        return near.group("corridor").strip(), loc or None, direction
    return None, text, direction


def enrich_corridors(rows: list) -> list:
    """Second pass: rows with no " near " get the longest known corridor that their text starts with,
    otherwise the text before " - " (so "Fanling Bypass - Westbound (1)" -> "Fanling Bypass")."""
    known = sorted({r["corridor"] for r in rows if r.get("corridor")}, key=len, reverse=True)
    for r in rows:
        if r.get("corridor"):
            continue
        text = r.get("location") or ""
        head = text.split(" - ")[0].strip()
        hit = next((k for k in known if head.lower().startswith(k.lower())), None)
        if hit:
            r["corridor"] = hit
            rest = head[len(hit):].strip()
            r["location"] = rest or None
        else:
            r["corridor"] = head or None
            r["location"] = None
    return rows


# ----------------------------------------------------------------------- CSV files
def _read_csv(source: Any) -> list:
    if isinstance(source, (str, Path)):
        text = Path(source).read_bytes().decode("utf-8-sig")  # utf-8-sig drops the BOM
    else:
        text = bytes(source).decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def _clean_key(key: Optional[str]) -> str:
    return (key or "").strip().lstrip("\ufeff").lower()


def _clean_row(row: dict) -> dict:
    return {_clean_key(k): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def _float(value: Any) -> Optional[float]:
    return _num(value.strip()) if isinstance(value, str) and value.strip() else None


def parse_detector_csv(source: Any) -> tuple:
    """Detector locations -> (rows, rejects). Accepts `AID_ID_Number` or the spec's `Device_ID`."""
    rows, rejects = [], []
    for raw in _read_csv(source):
        r = _clean_row(raw)
        det_id = r.get("aid_id_number") or r.get("device_id")
        if not det_id:
            rejects.append({"reason": "missing_detector_id", "record": raw})
            continue
        lat, lon = _float(r.get("latitude")), _float(r.get("longitude"))
        if lat is None or lon is None or not (22.0 <= lat <= 22.7 and 113.7 <= lon <= 114.5):
            rejects.append({"reason": "invalid_coordinates", "record": raw})
            continue
        rotation = _float(r.get("rotation"))
        corridor, location, travel = split_location(r.get("road_en"))
        rows.append({
            "detector_id": det_id,
            "district": normalise_district(r.get("district")),
            "road_en": r.get("road_en") or None,
            "corridor": corridor,
            "location": location,
            "travel_direction": travel,
            "road_tc": r.get("road_tc") or None,
            "road_sc": r.get("road_sc") or None,
            "easting": _float(r.get("easting")),
            "northing": _float(r.get("northing")),
            "latitude": lat,
            "longitude": lon,
            "direction": normalise_direction(r.get("direction")),
            "rotation": int(rotation) if rotation is not None else None,
        })
    return enrich_corridors(rows), rejects
