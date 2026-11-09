import os
import shutil
from pathlib import Path

import pytest

from hktraffic.config import Settings

REAL = Path(__file__).parent / "fixtures" / "real"
RAW_XML = REAL / "rawSpeedVol-all.xml"
DET_CSV = REAL / "traffic_speed_volume_occ_info.csv"


@pytest.fixture
def real():
    return {"raw": RAW_XML, "det_csv": DET_CSV}


@pytest.fixture
def settings(tmp_path):
    dsn = os.getenv("HKT_TEST_DSN") or os.getenv(
        "HKT_DSN", "postgresql://postgres:postgres@localhost:5432/hktraffic")
    return Settings(data_dir=tmp_path / "data", dsn=dsn, schema="hkt_test",
                    spark_shuffle_partitions=4, min_hour_coverage=0.0, min_file_ratio=0.0001)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("Java runtime not found")
    from hktraffic import spark_jobs

    s = Settings(data_dir=tmp_path_factory.mktemp("spark"), spark_shuffle_partitions=4)
    session = spark_jobs.get_spark(s, "hktraffic-tests")
    yield session
    session.stop()


@pytest.fixture
def pg(settings):
    """Clean PostgreSQL schema; skips (does not fail) when no database is reachable."""
    from hktraffic import serve

    try:
        conn = serve.connect(settings.dsn)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not available: {exc}")
    with conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {settings.schema} CASCADE")
    serve.init_schema(conn, settings.schema)
    yield conn
    with conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {settings.schema} CASCADE")
    conn.close()
