"""Minimal client for the data.gov.hk historical-archive API (documented at data.gov.hk/en/help/api-spec).

  list-file-versions?url=<file url>&start=YYYYMMDD&end=YYYYMMDD
      -> {"version-count": n, "total-size": bytes, "timestamps": ["20261028-0001", ...], ...}
  get-file?url=<file url>&time=YYYYMMDD-HHMM  -> the file as it was at that time (GMT+8)

Practical note: a full day is ~1000 files, so for whole days download the packaged daily zips from the
dataset page instead (see archive.py). This client is for sampling a few versions or checking what exists.
It has NOT been run against the live API by the author of this repo (offline sandbox): verify first.
"""
from __future__ import annotations

import time
from typing import Any

import requests

BASE = "https://api.data.gov.hk/v1/historical-archive"


def _get(endpoint: str, params: dict, timeout: float = 30.0, retries: int = 3) -> requests.Response:
    last = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f"{BASE}/{endpoint}", params=params, timeout=timeout)
            if resp.status_code in (400, 404):
                resp.raise_for_status()  # not retryable
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 404):
                raise
            last = exc
        except requests.RequestException as exc:
            last = exc
        if attempt < retries:
            time.sleep(2.0**attempt)
    raise RuntimeError(f"{endpoint} failed after {retries} attempts: {last}")


def list_file_versions(url: str, start: str, end: str, **kw) -> dict:
    return _get("list-file-versions", {"url": url, "start": start, "end": end}, **kw).json()


def get_file(url: str, timestamp: str, **kw) -> bytes:
    return _get("get-file", {"url": url, "time": timestamp}, **kw).content


def summarise_versions(payload: Any) -> dict:
    """Pick the useful numbers out of a list-file-versions response."""
    if not isinstance(payload, dict):
        raise ValueError("unexpected list-file-versions response")
    stamps = payload.get("timestamps") or []
    return {"version_count": payload.get("version-count", len(stamps)),
            "total_size_bytes": payload.get("total-size"),
            "first": stamps[0] if stamps else None, "last": stamps[-1] if stamps else None}
