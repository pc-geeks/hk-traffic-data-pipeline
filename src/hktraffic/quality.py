"""Quality gate for one archive day. `evaluate` is a pure function (easy to unit-test);
the Spark metric collection lives in spark_jobs.py."""
from __future__ import annotations

from .config import Settings

EXPECTED_FILES = {"raw": 1440}   # one raw file per minute
BAD_FLAGS = ("parse_error", "invalid_flag", "missing_value", "out_of_range")


class DataQualityError(RuntimeError):
    pass


def evaluate(metrics: dict, settings: Settings) -> list:
    """metrics: {'kind', 'files', 'rows', 'flags': {flag: count}, 'detectors_reporting', 'detectors_known'}"""
    failures = []
    kind = metrics["kind"]
    expected = EXPECTED_FILES[kind]
    ratio = metrics["files"] / expected
    if metrics["files"] == 0 or metrics["rows"] == 0:
        return [f"{kind}: no data"]
    if ratio < settings.min_file_ratio:
        failures.append(f"{kind}: only {metrics['files']} of {expected} expected files ({ratio:.0%}, "
                        f"minimum {settings.min_file_ratio:.0%})")
    flags = metrics["flags"]
    if flags.get("parse_error", 0):
        failures.append(f"{kind}: {flags['parse_error']} file(s) failed to parse")
    bad = sum(flags.get(f, 0) for f in BAD_FLAGS)
    share = bad / metrics["rows"]
    if share > settings.max_flagged_share:
        failures.append(f"{kind}: {share:.1%} of rows are flagged bad (limit {settings.max_flagged_share:.0%})")
    known = metrics.get("detectors_known")
    if kind == "raw" and known:
        reporting = metrics.get("detectors_reporting", 0)
        if reporting / known < 0.5:
            failures.append(f"raw: only {reporting} of {known} known detectors reported")
    return failures


def check(metrics: dict, settings: Settings) -> dict:
    failures = evaluate(metrics, settings)
    if failures:
        raise DataQualityError("; ".join(failures))
    return metrics
