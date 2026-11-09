-- Serving layer (PostgreSQL). "__SCHEMA__" is replaced with the configured schema (default: hkt).
-- Facts deliberately have NO foreign keys to the dimension: the real data contains detectors that are
-- missing from the dimension file. Dropping those rows would hide a data problem, so analytics use
-- LEFT JOINs and a query reports them.
CREATE SCHEMA IF NOT EXISTS __SCHEMA__;

CREATE TABLE IF NOT EXISTS __SCHEMA__.dim_detector (
    detector_id TEXT PRIMARY KEY,
    district TEXT, road_en TEXT, corridor TEXT, location TEXT, travel_direction TEXT,
    road_tc TEXT, road_sc TEXT,
    latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
    direction TEXT, rotation INT
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.fact_detector_hour (
    detector_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    hour INT NOT NULL,
    periods_seen INT, lane_rows INT, usable_rows INT, bad_rows INT,
    volume DOUBLE PRECISION, avg_occupancy DOUBLE PRECISION, congested_rows INT,
    wavg_speed DOUBLE PRECISION, congested_share DOUBLE PRECISION,
    coverage DOUBLE PRECISION, low_coverage BOOLEAN,
    iso_dow INT, day_type TEXT,
    PRIMARY KEY (detector_id, obs_date, hour)
);
CREATE INDEX IF NOT EXISTS ix_fdh_date ON __SCHEMA__.fact_detector_hour (obs_date);

CREATE TABLE IF NOT EXISTS __SCHEMA__.hotspot (
    detector_id TEXT PRIMARY KEY,
    mean_congested_share DOUBLE PRECISION, mean_speed_kmh DOUBLE PRECISION,
    days_observed INT, hours_observed INT,
    peak_hour INT, peak_hour_share DOUBLE PRECISION,
    rank INT
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.anomaly_hour (
    detector_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    hour INT NOT NULL,
    wavg_speed DOUBLE PRECISION, baseline_mean DOUBLE PRECISION, baseline_std DOUBLE PRECISION,
    baseline_days INT, z_score DOUBLE PRECISION, is_anomaly BOOLEAN,
    volume DOUBLE PRECISION, congested_share DOUBLE PRECISION,
    PRIMARY KEY (detector_id, obs_date, hour)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.pipeline_run (
    day DATE NOT NULL,
    kind TEXT NOT NULL,
    files INT, rows BIGINT,
    flags JSONB,
    detectors_reporting INT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (day, kind)
);

-- ------------------------------------------------------------------------------------------------
-- Views for Power BI (flat, readable names, LEFT JOINs so detectors missing from the dimension stay visible).
-- Boolean columns are exposed as 0/1 because a PostgreSQL CSV export writes t/f, which Power BI imports as text.
-- Weekday names are computed from iso_dow so they do not depend on the database locale.
-- ------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW __SCHEMA__.bi_detector_hour AS
SELECT f.detector_id,
       COALESCE(d.corridor, '(unknown detector)') AS corridor,
       d.location, d.travel_direction,
       COALESCE(d.district, '(unknown)') AS district,
       d.latitude, d.longitude,
       f.obs_date, f.hour, f.iso_dow,
       (ARRAY['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])[f.iso_dow] AS weekday,
       f.day_type,
       ROUND(f.wavg_speed::numeric, 2)::DOUBLE PRECISION AS wavg_speed, f.volume, ROUND(f.avg_occupancy::numeric, 2)::DOUBLE PRECISION AS avg_occupancy,
       ROUND(f.congested_share::numeric, 4)::DOUBLE PRECISION AS congested_share,
       f.congested_rows, f.usable_rows, f.lane_rows, f.bad_rows,
       ROUND(f.coverage::numeric, 4)::DOUBLE PRECISION AS coverage, CASE WHEN f.low_coverage THEN 1 ELSE 0 END AS low_coverage
FROM __SCHEMA__.fact_detector_hour f
LEFT JOIN __SCHEMA__.dim_detector d USING (detector_id);

CREATE OR REPLACE VIEW __SCHEMA__.bi_corridor_hour AS
SELECT COALESCE(d.corridor, '(unknown detector)') AS corridor,
       COALESCE(d.travel_direction, '(unknown)') AS travel_direction,
       f.obs_date, f.hour, f.iso_dow,
       (ARRAY['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])[f.iso_dow] AS weekday,
       f.day_type,
       COUNT(DISTINCT f.detector_id) AS detectors,
       ROUND((SUM(f.wavg_speed * f.volume) FILTER (WHERE f.wavg_speed IS NOT NULL)
         / NULLIF(SUM(f.volume) FILTER (WHERE f.wavg_speed IS NOT NULL), 0))::numeric, 2)::DOUBLE PRECISION AS wavg_speed,
       SUM(f.volume) AS volume,
       ROUND((SUM(f.congested_rows)::DOUBLE PRECISION / NULLIF(SUM(f.usable_rows), 0))::numeric, 4)::DOUBLE PRECISION AS congested_share
FROM __SCHEMA__.fact_detector_hour f
LEFT JOIN __SCHEMA__.dim_detector d USING (detector_id)
WHERE NOT f.low_coverage
GROUP BY 1, 2, f.obs_date, f.hour, f.iso_dow, f.day_type;

CREATE OR REPLACE VIEW __SCHEMA__.bi_hotspot AS
SELECT h.rank, h.detector_id,
       COALESCE(d.corridor, '(unknown detector)') AS corridor, d.location, d.travel_direction,
       COALESCE(d.district, '(unknown)') AS district, d.latitude, d.longitude,
       ROUND(h.mean_congested_share::numeric, 4)::DOUBLE PRECISION AS mean_congested_share, ROUND(h.mean_speed_kmh::numeric, 2)::DOUBLE PRECISION AS mean_speed_kmh,
       h.days_observed, h.hours_observed, h.peak_hour, ROUND(h.peak_hour_share::numeric, 4)::DOUBLE PRECISION AS peak_hour_share
FROM __SCHEMA__.hotspot h
LEFT JOIN __SCHEMA__.dim_detector d USING (detector_id);

CREATE OR REPLACE VIEW __SCHEMA__.bi_anomaly AS
SELECT a.obs_date, a.hour, a.detector_id,
       COALESCE(d.corridor, '(unknown detector)') AS corridor, d.location, d.travel_direction,
       COALESCE(d.district, '(unknown)') AS district, d.latitude, d.longitude,
       ROUND(a.wavg_speed::numeric, 2)::DOUBLE PRECISION AS wavg_speed, ROUND(a.baseline_mean::numeric, 2)::DOUBLE PRECISION AS baseline_mean, ROUND(a.baseline_std::numeric, 2)::DOUBLE PRECISION AS baseline_std,
       a.baseline_days,
       ROUND(((a.baseline_mean - a.wavg_speed) / NULLIF(a.baseline_mean, 0))::numeric, 4)::DOUBLE PRECISION AS drop_share,
       ROUND(a.z_score::numeric, 2)::DOUBLE PRECISION AS z_score, CASE WHEN a.is_anomaly THEN 1 ELSE 0 END AS is_anomaly, a.volume
FROM __SCHEMA__.anomaly_hour a
LEFT JOIN __SCHEMA__.dim_detector d USING (detector_id);

-- Data trust: how much of each hour's data exists, per day (the archive really has gaps)
CREATE OR REPLACE VIEW __SCHEMA__.bi_coverage_hour AS
SELECT obs_date, hour,
       COUNT(DISTINCT detector_id) AS detectors_reporting,
       ROUND(AVG(coverage)::numeric, 4)::DOUBLE PRECISION AS avg_coverage,
       ROUND(AVG(CASE WHEN low_coverage THEN 1.0 ELSE 0.0 END)::numeric, 4)::DOUBLE PRECISION AS low_coverage_share
FROM __SCHEMA__.fact_detector_hour
GROUP BY obs_date, hour;

-- Data trust: which detectors produce the most rows flagged bad
CREATE OR REPLACE VIEW __SCHEMA__.bi_detector_quality AS
SELECT f.detector_id,
       COALESCE(d.corridor, '(unknown detector)') AS corridor, d.location,
       COALESCE(d.district, '(unknown)') AS district, d.latitude, d.longitude,
       SUM(f.lane_rows) AS lane_rows, SUM(f.bad_rows) AS bad_rows,
       ROUND((SUM(f.bad_rows)::DOUBLE PRECISION / NULLIF(SUM(f.lane_rows), 0))::numeric, 4)::DOUBLE PRECISION AS bad_share
FROM __SCHEMA__.fact_detector_hour f
LEFT JOIN __SCHEMA__.dim_detector d USING (detector_id)
GROUP BY f.detector_id, d.corridor, d.location, d.district, d.latitude, d.longitude;

CREATE OR REPLACE VIEW __SCHEMA__.bi_pipeline_run AS
SELECT day, kind, files, rows, detectors_reporting,
       (flags->>'ok')::BIGINT AS ok_rows,
       COALESCE((flags->>'invalid_flag')::BIGINT, 0) AS invalid_rows,
       COALESCE((flags->>'out_of_range')::BIGINT, 0) AS out_of_range_rows,
       COALESCE((flags->>'no_vehicles')::BIGINT, 0) AS no_vehicle_rows,
       COALESCE((flags->>'standstill_candidate')::BIGINT, 0) AS standstill_rows,
       COALESCE((flags->>'parse_error')::BIGINT, 0) AS parse_error_rows
FROM __SCHEMA__.pipeline_run;
