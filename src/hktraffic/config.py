"""Settings from environment variables (HKT_*) with safe defaults."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# Real file URLs (from the dataset page on data.gov.hk). Used as the `url` of the historical-archive API.
RAW_URL = "https://resource.data.one.gov.hk/td/traffic-detectors/rawSpeedVol-all.xml"


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path("data")
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hktraffic"
    schema: str = "hkt"
    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = 16
    spark_driver_memory: str = "2g"          # local mode: the driver JVM does all the work
    # --- analytics definitions. These are heuristics, documented in the README, not official standards.
    congested_speed_kmh: float = 20.0       # lane-period counts as congested when speed is below this ...
    congested_min_occupancy: float = 15.0   # ... and occupancy (%) is at least this
    max_plausible_speed_kmh: float = 150.0  # above this a lane reading is flagged as an outlier
    anomaly_z_threshold: float = -3.0
    anomaly_min_baseline_days: int = 4
    anomaly_min_relative_drop: float = 0.25   # ... AND at least this fraction below the baseline mean
    anomaly_std_floor_ratio: float = 0.05     # baseline std is floored at this fraction of the mean (tiny samples)
    min_hour_coverage: float = 0.5          # share of the 120 expected 30s periods needed in an hour
    # --- quality gate
    min_file_ratio: float = 0.5             # real archive day had 1035/1440 raw files (72%)
    max_flagged_share: float = 0.05         # share of lane rows that are not 'ok'

    def __post_init__(self):
        if not _SCHEMA_RE.match(self.schema):
            raise ValueError(f"invalid schema name: {self.schema!r}")

    @property
    def landing(self) -> Path:
        return self.data_dir / "landing"

    @property
    def raw(self) -> Path:
        return self.data_dir / "raw"

    @property
    def silver(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold(self) -> Path:
        return self.data_dir / "gold"


def get_settings() -> Settings:
    d = Settings()
    e = os.getenv
    return Settings(
        data_dir=Path(e("HKT_DATA_DIR", str(d.data_dir))),
        dsn=e("HKT_DSN", d.dsn),
        schema=e("HKT_SCHEMA", d.schema),
        spark_master=e("HKT_SPARK_MASTER", d.spark_master),
        spark_shuffle_partitions=int(e("HKT_SHUFFLE_PARTITIONS", d.spark_shuffle_partitions)),
        spark_driver_memory=e("HKT_SPARK_MEMORY", d.spark_driver_memory),
        congested_speed_kmh=float(e("HKT_CONGESTED_SPEED", d.congested_speed_kmh)),
        congested_min_occupancy=float(e("HKT_CONGESTED_OCCUPANCY", d.congested_min_occupancy)),
        max_plausible_speed_kmh=float(e("HKT_MAX_SPEED", d.max_plausible_speed_kmh)),
        anomaly_z_threshold=float(e("HKT_ANOMALY_Z", d.anomaly_z_threshold)),
        anomaly_min_baseline_days=int(e("HKT_ANOMALY_MIN_DAYS", d.anomaly_min_baseline_days)),
        anomaly_min_relative_drop=float(e("HKT_ANOMALY_MIN_DROP", d.anomaly_min_relative_drop)),
        anomaly_std_floor_ratio=float(e("HKT_ANOMALY_STD_FLOOR", d.anomaly_std_floor_ratio)),
        min_hour_coverage=float(e("HKT_MIN_HOUR_COVERAGE", d.min_hour_coverage)),
        min_file_ratio=float(e("HKT_MIN_FILE_RATIO", d.min_file_ratio)),
        max_flagged_share=float(e("HKT_MAX_FLAGGED_SHARE", d.max_flagged_share)),
    )
