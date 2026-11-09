"""Daily traffic pipeline for one day of Transport Department archives.

    wait_for_archives -> unzip -> silver -> quality_gate -> gold -> serve

The historical archives (daily raw zips) are downloaded from data.gov.hk and dropped into data/landing/.
The first task waits until the raw zip for the run's date exists.

Each task shells out to the `hktraffic` CLI, which keeps the DAG thin and lets you run every step by hand.
Every step is idempotent (overwrites its own day only), so a retry or a manual re-run is always safe.
Needs Java + PySpark in the Airflow image: see docker/airflow.Dockerfile (not tested in the dev sandbox).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

from hktraffic import archive
from hktraffic.config import get_settings


def _archives_ready(ds: str, **_) -> bool:
    found = archive.find_zips(get_settings().landing, datetime.strptime(ds, "%Y-%m-%d").date())
    return bool(found["raw"])


with DAG(
    dag_id="hk_traffic_daily",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["traffic", "spark"],
) as dag:
    wait = PythonSensor(task_id="wait_for_archives", python_callable=_archives_ready,
                        poke_interval=1800, timeout=12 * 3600, mode="reschedule")
    unzip = BashOperator(task_id="unzip", bash_command="python -m hktraffic unzip --date {{ ds }}")
    silver = BashOperator(task_id="silver", bash_command="python -m hktraffic silver --date {{ ds }}")
    gate = BashOperator(task_id="quality_gate", bash_command="python -m hktraffic check --date {{ ds }}")
    gold = BashOperator(task_id="gold", bash_command="python -m hktraffic gold --start {{ ds }} --end {{ ds }}")
    serve = BashOperator(task_id="serve", bash_command="python -m hktraffic serve")

    wait >> unzip >> silver >> gate >> gold >> serve

if __name__ == "__main__":
    dag.test()
