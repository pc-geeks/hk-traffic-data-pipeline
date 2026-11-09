# Airflow image with a Java runtime and PySpark, so the DAG can run Spark in local mode.
# NOT tested in the development sandbox (no Docker there): build it and confirm the DAG imports.
FROM apache/airflow:2.9.3
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3
