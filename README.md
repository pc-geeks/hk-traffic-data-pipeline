# hk-traffic-pipeline

Hong Kong measures the speed of its major roads all day and publishes the readings; this project turns that public record into a trustworthy view of where the city actually congests and which hours were unusually slow.

A PySpark pipeline on **Hong Kong road-traffic open data** (Transport Department, via data.gov.hk): raw XML
archives -> cleaned Parquet with explicit quality flags -> analytics tables -> PostgreSQL, orchestrated with Airflow.

It is built to answer questions, not just move data:

1. **Where and when are Hong Kong's roads slowest?** (congestion hotspots by detector and hour)
2. **Which roads were unusually slow today compared with a normal weekday?** (anomaly detection against a per-detector baseline)
3. **How trustworthy is the data?** (every row keeps a quality flag, and the real archive has gaps and traps that the pipeline surfaces)

## Why Spark (and when it is not worth it)

From the real archive of 2026-10-28: **1,035 raw XML files (37.2 MB zipped) per day**. One raw file is ~690 KB of XML with 4,140 lane rows, so a day is roughly 700 MB of XML and ~4 million rows
(~130 million rows a month, estimated from the sample). XML is not columnar, so parsing dominates and it parallelises
per file: a good fit for Spark's `binaryFiles` plus partitioned Parquet output.

The honest part: **for a single day, Spark is not obviously needed.** In a smoke test on a 1-core machine, a plain
Python loop parsed a synthetic file in ~46 ms, so a day takes about a minute on one core. The case for Spark rests on
re-processing **months of history**, on **multiple cores or a cluster**, and on the columnar Parquet layers it produces.
`hktraffic benchmark` lets you measure where the crossover is on your own machine (see [Benchmark](#benchmark)).

## Architecture

```mermaid
flowchart LR
    Z[Daily zip archives<br/>data.gov.hk] --> L[data/landing]
    L -->|unzip, idempotent| R[(data/raw<br/>XML per snapshot)]
    R -->|Spark: parse + flag| S[(Silver Parquet<br/>lane_30s)]
    D[Detector CSV] --> DIM[(dim_detector)]
    S --> Q{Quality gate}
    Q -->|pass| G[(Gold Parquet<br/>detector_hour, hotspots, anomalies)]
    G --> P[(PostgreSQL<br/>serving layer)]
    DIM --> P
    A[Airflow] -.-> L
    A -.-> Q
```

| Layer | Content |
|---|---|
| Raw | The XML files exactly as published, partitioned `dt=YYYY-MM-DD` |
| Silver | One row per detector lane per 30 s (`lane_30s`), **every row kept with a `quality_flag`** |
| Gold | `detector_hour` (volume-weighted speed, occupancy, congested share, coverage), `hotspots`, `anomalies` |
| Serving | The small gold tables and the dimensions in PostgreSQL, with example SQL |

## What the real data looks like

The specification and the real files disagree in several places, and the real files contain traps that would
silently corrupt averages (invalid rows with plausible numbers, speeds above 200 km/h, rows with no vehicles but a
speed).
The same cases are asserted by tests against the real sample files in `tests/fixtures/real/`.

Quality flags on lane rows: `ok`, `invalid_flag`, `missing_value`, `out_of_range`, `no_vehicles`,
`standstill_candidate`, `parse_error`. A corrupt or truncated XML file produces one `parse_error` row instead of
failing the job or vanishing.

## Analytics definitions (heuristics, not official standards)

- **Speed** is averaged **weighted by vehicle volume**, over `ok` rows only.
- **Congested lane-period**: speed < 20 km/h and occupancy >= 15%, or a `standstill_candidate` (a vehicle sitting on the
  detector with zero throughput). `congested_share` = congested rows / usable rows in the hour.
- **Coverage**: periods actually seen in the hour / 120. Hours below 50% are flagged `low_coverage` and excluded
  from hotspots and anomalies (the real archive misses about 28% of the raw minutes).
- **Anomaly**: for the same detector, hour and day type (weekday/weekend) on other days, build a baseline (mean, standard
  deviation, at least 4 days). An hour is an anomaly when its z-score is <= -3 **and** its speed is at least 25% below the
  baseline mean. The standard deviation is floored at 5% of the mean. The extra conditions exist because a plain z <= -3
  rule with a 5-day baseline flagged about 2.6% of all detector-hours on noise alone (found with the synthetic test data).
  Public holidays are not modelled.

All thresholds live in `config.py` and can be set with `HKT_*` environment variables.

## Quick start

Requirements: Python 3.9–3.12 and Java 8, 11, 17, or 21 (PySpark 3.5.3). Docker is only for PostgreSQL and Airflow.

```bash
python -m venv .venv && source .venv/bin/activate
make install                       # pip install -e ".[dev]"
make up                            # PostgreSQL on localhost:5432
export HKT_DSN=postgresql://postgres:postgres@localhost:5432/hktraffic
```

1. Download from the dataset page on data.gov.hk (section *Download Historical Data*) into `data/landing/`:
   the daily **Raw Data** zip, and the latest monthly zip of *Locations of Traffic Detectors*.
   `scan` only reads `data/landing`. `data/raw` is the folder unzip fills with XML.
2. Check what was recognised (package names can be opaque hashes, so files are identified by their content), build the
   dimensions once, then run the days:

```bash
hktraffic scan                                        # lists days, dimension zips, anything unrecognised
hktraffic dims                                        # newest detector zip found in data/landing
hktraffic run-range --start 2026-11-02 --end 2026-11-06 --delete-raw
hktraffic serve                                       # gold + dimensions + run metrics -> PostgreSQL
hktraffic export-bi --out powerbi/bi_data             # CSV files for the Power BI report
```

`run-range` uses one Spark session for all days (unzip, silver, quality gate per day, then gold) and `--delete-raw` removes
each day's extracted XML afterwards (about 1 GB a day). `hktraffic run-day --date ...` does a single day.
`scan`, `dims`, and `run-range` need the venv and Java only. `serve` and `export-bi` talk to PostgreSQL: start Docker
Desktop, then `make up` (or `docker compose up -d warehouse`). `export-bi` copies the `bi_*` views out of that database.
`serve` is upsert-only: to remove old rows (for example after a rerun that changed the set of days) drop the schema first.

Anomaly detection needs a baseline: process at least five weekdays before expecting any anomalies.
Example queries on the serving layer: [sql/analytics.sql](sql/analytics.sql) (CTEs, `RANK`, `LAG`, `FILTER`, LEFT JOINs).

### Airflow

```bash
echo "AIRFLOW_UID=$(id -u)" > .env       # Linux only
docker compose up -d --build            # Airflow image with Java + PySpark
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

`hk_traffic_daily`: `wait_for_archives -> unzip -> silver -> quality_gate -> gold -> serve`. The sensor waits until
the raw zip for the run date is in `data/landing/`. Every step is idempotent (Parquet writes use dynamic partition
overwrite, PostgreSQL loads are upserts), so retries and re-runs never duplicate data.

## Dashboard (Power BI)

The last step of the pipeline is a Power BI report built from the serving layer. Seven flat views (`bi_*`) in PostgreSQL
are exported to CSV with one command:

```bash
hktraffic export-bi --out powerbi/bi_data
```

`export-bi` writes to `powerbi/bi_data`, which is the directory `powerbi/HKTraffic.pbip` reads
(`DataPath`). Refresh the report after exporting.

| Page | Question |
|---|---|
| 1. The rhythm of a day | How do speed and congestion change by hour, weekday vs weekend? |
| 2. Hotspots | Where is congestion concentrated? (map of detectors, ranked corridors) |
| 3. Tidal flow | Do the two directions of a corridor jam at different times? |
| 4. Anomalies | Which hours were far slower than usual for that detector, and do they match real events? |
| 5. Data trust | How much of the data can be trusted? (archive gaps, bad-row share per detector) |

The screenshots below are the report open in Power BI Desktop on five weekdays, 2–6 November 2026.

![Weekday speed by hour, with a congestion heatmap](screenshots/Screenshot%202026-11-09%20085206.png)

*The rhythm of a day. One weekday line: slowest at 08:00 and 18:00.*

![Detector locations and the ten corridors with the highest average congestion](screenshots/Screenshot%202026-11-09%20085307.png)

*Hotspots. Each point is a detector; the bars are the ten corridors with the highest average congested share.*

![Tuen Mun Road speed and volume by direction and hour](screenshots/Screenshot%202026-11-09%20085451.png)

*Tidal flow on Tuen Mun Road. Eastbound is slower in the morning; westbound is slower in the evening.*

![Anomaly table and counts by hour](screenshots/Screenshot%202026-11-09%20085527.png)

*Anomalies. 272 detector-hours were much slower than that detector's usual speed for the same hour.*

![Archive coverage by hour and the detectors with the most bad rows](screenshots/Screenshot%202026-11-09%20085604.png)

*Data trust. About 74% of the expected raw files arrived; one detector flags 66% of its rows as bad.*

## Findings

The screenshots above are five weekdays, Monday 2026-11-02 to Friday 2026-11-06: 5,321 raw files, 22.0 million lane
readings, 92,847 detector-hours (783 detectors with data, out of 807 in the location file).

- **18:00 is the slowest hour**: 65.3 km/h, with 3.5% of lane readings congested. Against midday at 72.4 km/h (11:00–14:59),
  the morning peak is 5.7% slower and the evening peak 5.9% slower. Across the week the average speed is 71.6 km/h, and 1.2%
  of usable lane readings are congested.
- **Congestion is concentrated**: 5 of 80 corridors hold 32% of all congested readings, the top 10 hold 51%, and 628 of 783
  detectors (80%) are congested less than 1% of the time.
- **Tidal flow is visible on Tuen Mun Road**: eastbound is 9.3% slower than midday in the morning and 1.1% slower in the
  evening; westbound is not slower in the morning and is 6.5% slower in the evening.
- **A high-volume slowdown stands out**: Lion Rock Tunnel Road near Fung Shing Court on Thursday 5 November, 16:00–17:00,
  about 9 km/h against about 80 km/h usually. At 09:00 that same day, eight detectors on Island Eastern Corridor flagged
  together. Rows with only one or two vehicles can rank at the top of the drop table; those are not a jam.
- **Data trust**: 72–76% of the expected raw files arrived (73.9% over the five days). Bad rows are 0.81% of lane readings.
  TDS90018 on Tuen Mun Road flags 66% of its rows as bad. The thinnest hour is 3 November at 18:00, with 61% coverage.

## Benchmark

```bash
hktraffic benchmark --date 2026-10-28
```

Runs the same computation (volume-weighted speed per detector-hour) single-process in Python and in Spark on the raw
files of a day and prints files, MB, cores and seconds.

| Files | XML size | Cores | Python (s) | Spark (s) | Speed-up |
|---|---|---|---|---|---|
| _fill in with your own measurements_ | | | | | |

Development smoke test (1 CPU core, 24 **synthetic** files, 17 MB): Python 1.1 s, Spark 23.7 s (JVM startup
dominates, no parallelism to gain), identical row counts and groups. Do not quote this as a result: it only shows
that Spark needs scale or cores to pay off. Replace it with your own numbers.

## Tests

```bash
make test        # 60 tests: parsers on real files, archive handling, quality gate, Spark jobs, PostgreSQL
make lint
```

Spark tests check the silver output against an independent pure-Python calculation on the real sample (flag counts
must match exactly), idempotent re-runs, corrupt-file handling, and the gold logic on **synthetic** multi-day data
(`synthetic.py`: rush-hour hotspots and one injected incident, used only for testing and benchmarking).
CI (GitHub Actions) runs lint and the full test suite with Java and PostgreSQL.

## Project layout

```
src/hktraffic/   config, parsing, archive, archive_api, spark_jobs, quality, serve, synthetic, benchmark, cli
dags/            hk_traffic_daily.py
sql/             analytics.sql (serving layer)
powerbi/         HKTraffic.pbip, theme.json, build_pbip.py
docker/          airflow.Dockerfile (Airflow + Java + PySpark)
tests/           unit + Spark + PostgreSQL tests, real fixtures
```

## Limitations and next steps

- Daily archives are downloaded by hand; automating it with the historical-archive API means ~1,000 requests a day.
- Segment geometry is not in these files (it is in a separate road-network dataset), so maps use detector coordinates.
- Rain impact (joining hourly rainfall) is not implemented: it needs historical rainfall, which has to be located first.
- Holidays and typhoon signals are not modelled in the anomaly baseline.
- Airflow runs in `standalone` mode (SQLite metadata): fine for a demo, not for production.

## Data source

Transport Department, *Traffic Data of Strategic / Major Roads*, https://data.gov.hk/en-data/dataset/hk-td-sm_4-traffic-data-strategic-major-roads
Check the data.gov.hk terms before reuse and keep this attribution.
