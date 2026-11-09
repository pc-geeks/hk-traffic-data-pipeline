"""Landing zone: daily zip archives downloaded from data.gov.hk -> extracted raw XML files.

The daily package looks like (real name seen on the dataset page):
  Traffic Speed, Volume and Road Occupancy (Raw Data) - 20261028.zip        (1035 files, 37.2 MB)
Files are classified by the words "Raw Data" and the 8-digit date in the zip name, and
extracted to  data/raw/raw/dt=YYYY-MM-DD/<file>.xml . Extraction is idempotent and zip-slip safe.
"""
from __future__ import annotations

import re
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

_DATE_RE = re.compile(r"(?<!\d)(\d{8})(?!\d)")
_MONTH_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")


def classify_zip(name: str) -> Optional[tuple]:
    """'... (Raw Data) - 20261028.zip' -> ('raw', date(2026, 10, 28)); None when not recognised."""
    low = name.lower()
    if "raw" not in low:
        return None
    kind = "raw"
    m = _DATE_RE.search(name)
    if not m:
        return None
    try:
        return kind, datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def peek_kind(path: Path) -> Optional[str]:
    """Identify a zip by its CONTENT: 'raw' (XML root tag) or 'detectors' (CSV header).

    Needed because the packages downloaded from data.gov.hk can have opaque names such as
    '7c383a97...-20261026.zip' (a hash of the file URL plus the date), with no 'Raw Data' in the name.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                low = info.filename.lower()
                if info.is_dir():
                    continue
                if low.endswith(".xml"):
                    with zf.open(info) as f:
                        head = f.read(4096).decode("utf-8", "ignore")
                    if "<raw_speed_volume_list" in head:
                        return "raw"
                    return None
                if low.endswith(".csv"):
                    with zf.open(info) as f:
                        head = f.read(512).decode("utf-8-sig", "ignore").lower()
                    if "aid_id_number" in head or "device_id" in head:
                        return "detectors"
                    return None
    except zipfile.BadZipFile:
        return None
    return None


def classify_path(path: Path) -> Optional[tuple]:
    """('raw', day) from the file name when possible, otherwise from the zip content."""
    c = classify_zip(Path(path).name)
    if c:
        return c
    m = _DATE_RE.search(Path(path).name)
    if not m:
        return None
    try:
        day = datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None
    kind = peek_kind(Path(path))
    return (kind, day) if kind == "raw" else None


def find_zips(landing: Path, day: date) -> dict:
    """{'raw': [paths]} of the raw-data zips in the landing folder for that day."""
    found = {"raw": []}
    for p in sorted(Path(landing).glob("*.zip")):
        c = classify_path(p)
        if c and c[1] == day:
            found[c[0]].append(p)
    return found


def scan_landing(landing: Path) -> dict:
    """Everything recognisable in the landing folder: {'days': {day: {'raw': [...]}},
    'detectors': [...], 'unknown': [...]}. Dimension zips are sorted by their 6-digit month."""
    out = {"days": {}, "detectors": [], "unknown": []}
    for p in sorted(Path(landing).glob("*.zip")):
        c = classify_path(p)
        if c:
            out["days"].setdefault(c[1], {"raw": []})[c[0]].append(p)
            continue
        kind = peek_kind(p)
        if kind == "detectors":
            out[kind].append(p)
        else:
            out["unknown"].append(p)
    out["detectors"].sort(key=lambda p: (_MONTH_RE.search(p.name).group(1) if _MONTH_RE.search(p.name) else "", p.name))
    return out


def day_dir(raw_dir: Path, kind: str, day: date) -> Path:
    return Path(raw_dir) / kind / f"dt={day:%Y-%m-%d}"


def unzip_day(zip_path: Path, dest: Path) -> dict:
    """Extract the .xml members of a zip into `dest` (flat). Existing files of equal size are skipped."""
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    extracted = skipped = ignored = 0
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".xml"):
                ignored += 1
                continue
            name = Path(info.filename).name  # basename only: no path traversal ("zip slip")
            if not name or name.startswith("."):
                ignored += 1
                continue
            target = dest / name
            if target.exists() and target.stat().st_size == info.file_size:
                skipped += 1
                continue
            tmp = target.with_suffix(".part")
            with zf.open(info) as src, open(tmp, "wb") as out:
                while chunk := src.read(1 << 20):
                    out.write(chunk)
            tmp.replace(target)
            extracted += 1
    return {"zip": Path(zip_path).name, "extracted": extracted, "skipped": skipped, "ignored": ignored}


def read_csv_bytes(path: Path) -> bytes:
    """A dimension file given either as .csv or as a .zip containing one csv (monthly packages)."""
    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise ValueError(f"{path} contains no .csv file")
            return zf.read(names[0])
    return path.read_bytes()
