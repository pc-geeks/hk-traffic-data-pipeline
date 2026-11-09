"""SYNTHETIC data generator for tests and benchmarks. NOT real traffic.

It takes a real raw file as a template (so structure, detector ids, lanes and quirks are realistic) and
produces many files with different timestamps, using a simple, documented model:

  * ~10% of detectors are "hotspots": on weekdays at 07-09 and 17-19 their speed drops to ~35% of normal,
    volume rises and occupancy rises
  * everything else gets +-8% noise
  * optionally one detector gets an "incident" (speed x0.2) for a few hours on one day, so the anomaly
    detector has something it must find
  * ~0.5% of lanes are marked invalid, ~0.2% get an extreme speed (outlier)

Use it to test the pipeline at a scale you do not want to download, and to look at how Spark behaves.
Never describe its output as real traffic in a README or CV.
"""
from __future__ import annotations

import random
import xml.etree.ElementTree as ET
import zlib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional


def _is_hotspot(detector_id: str) -> bool:
    return zlib.crc32(detector_id.encode()) % 10 == 0


def _fmt(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else f"{x:.1f}"


def make_files(template: Path, out_dir: Path, day: date, minutes: list, seed: int = 7,
               incident: Optional[tuple] = None) -> int:
    """Write one raw XML per minute in `minutes` (each a (hour, minute) tuple). Returns the number written.

    incident: (detector_id, start_hour, end_hour) -> speed x0.2 inside that window.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tree = ET.parse(template)
    root = tree.getroot()
    rnd = random.Random(f"{seed}-{day.isoformat()}")
    weekday = day.weekday() < 5
    base = {}
    for p in root.find("periods"):
        for d in p.find("detectors"):
            for lane in d.find("lanes"):
                key = (id(lane),)
                base[key] = (float(lane.findtext("speed")), float(lane.findtext("occupancy")),
                             float(lane.findtext("volume")))
    written = 0
    for hour, minute in minutes:
        root.find("date").text = day.isoformat()
        rush = weekday and hour in (7, 8, 17, 18)
        for pi, period in enumerate(root.find("periods")):
            start = datetime(day.year, day.month, day.day, hour, minute, 30 * pi)
            period.find("period_from").text = start.strftime("%H:%M:%S")
            period.find("period_to").text = (start + timedelta(seconds=30)).strftime("%H:%M:%S")
            for det in period.find("detectors"):
                det_id = det.findtext("detector_id")
                hot = _is_hotspot(det_id)
                inc = incident and det_id == incident[0] and incident[1] <= hour < incident[2]
                for lane in det.find("lanes"):
                    sp0, oc0, vo0 = base[(id(lane),)]
                    noise = rnd.uniform(0.92, 1.08)
                    factor = 0.35 if (hot and rush) else 1.0
                    if inc:
                        factor = 0.2
                    speed = min(sp0, 110) * factor * noise
                    occ = min(100.0, oc0 * (3.0 if (hot and rush) else 1.0) * noise + (25 if inc else 0))
                    vol = max(0.0, vo0 * (1.5 if (hot and rush) else 1.0) * noise)
                    lane.find("speed").text = _fmt(round(speed))
                    lane.find("occupancy").text = _fmt(round(occ))
                    lane.find("volume").text = _fmt(round(vol))
                    r = rnd.random()
                    lane.find("valid").text = "N" if r < 0.005 else "Y"
                    if 0.005 <= r < 0.007:
                        lane.find("speed").text = "200"
        name = f"rawSpeedVol-{day:%Y%m%d}-{hour:02d}{minute:02d}.xml"
        tree.write(out_dir / name, encoding="utf-8", xml_declaration=True)
        written += 1
    return written


def hourly_minutes(per_hour: int = 1) -> list:
    """per_hour files in every hour of the day, evenly spaced."""
    step = max(1, 60 // per_hour)
    return [(h, m) for h in range(24) for m in range(0, 60, step)][: 24 * per_hour]
