-- Analytics on the serving layer (schema: hkt).
-- Comments avoid semicolons so a simple splitter can run this file in tests.

-- 1. Top 10 congestion hotspots with road name and district
--    Concepts: LEFT JOIN to a dimension so detectors missing from the dimension file are kept
SELECT h.rank, h.detector_id, COALESCE(d.road_en, '(unknown detector)') AS road, d.district,
       ROUND((100 * h.mean_congested_share)::numeric, 1) AS congested_pct,
       ROUND(h.mean_speed_kmh::numeric, 1) AS mean_speed_kmh, h.peak_hour
FROM hkt.hotspot h
LEFT JOIN hkt.dim_detector d USING (detector_id)
ORDER BY h.rank, h.detector_id
LIMIT 10;

-- hkt is schema

-- 2. Which districts are most congested in each hour of the day?
--    Concepts: CTE + RANK() OVER (PARTITION BY hour)
WITH district_hour AS (
    SELECT d.district, f.hour, AVG(f.congested_share) AS share
    FROM hkt.fact_detector_hour f
    JOIN hkt.dim_detector d USING (detector_id)
    WHERE NOT f.low_coverage AND f.congested_share IS NOT NULL
    GROUP BY d.district, f.hour
), ranked AS (
    SELECT *, RANK() OVER (PARTITION BY hour ORDER BY share DESC) AS rnk FROM district_hour
)
SELECT hour, district, ROUND((100 * share)::numeric, 1) AS congested_pct
FROM ranked WHERE rnk = 1 ORDER BY hour;

-- 3. Weekday versus weekend speed by hour for the territory
--    Concepts: conditional aggregation with FILTER, volume weighting is already inside wavg_speed
SELECT hour,
       ROUND((AVG(wavg_speed) FILTER (WHERE day_type = 'weekday'))::numeric, 1) AS weekday_kmh,
       ROUND((AVG(wavg_speed) FILTER (WHERE day_type = 'weekend'))::numeric, 1) AS weekend_kmh
FROM hkt.fact_detector_hour
WHERE NOT low_coverage AND wavg_speed IS NOT NULL
GROUP BY hour ORDER BY hour;

-- 4. Hour-to-hour speed drop at each detector, largest drops first
--    Concepts: LAG() OVER (PARTITION BY detector, date ORDER BY hour)
WITH h AS (
    SELECT detector_id, obs_date, hour, wavg_speed,
           LAG(wavg_speed) OVER (PARTITION BY detector_id, obs_date ORDER BY hour) AS prev_speed
    FROM hkt.fact_detector_hour
    WHERE NOT low_coverage AND wavg_speed IS NOT NULL
)
SELECT detector_id, obs_date, hour, ROUND(prev_speed::numeric, 1) AS prev_kmh, ROUND(wavg_speed::numeric, 1) AS kmh,
       ROUND((wavg_speed - prev_speed)::numeric, 1) AS change_kmh
FROM h WHERE prev_speed IS NOT NULL
ORDER BY change_kmh ASC LIMIT 10;

-- 5. Anomalies (much slower than usual for that detector, day type and hour)
SELECT a.obs_date, a.hour, a.detector_id, COALESCE(d.road_en, '(unknown detector)') AS road,
       ROUND(a.wavg_speed::numeric, 1) AS kmh, ROUND(a.baseline_mean::numeric, 1) AS usual_kmh,
       ROUND(a.z_score::numeric, 1) AS z
FROM hkt.anomaly_hour a
LEFT JOIN hkt.dim_detector d USING (detector_id)
WHERE a.is_anomaly
ORDER BY a.z_score ASC LIMIT 20;

-- 6. Data completeness: how many detector-hours had too little data, per day
SELECT obs_date, COUNT(*) AS detector_hours,
       COUNT(*) FILTER (WHERE low_coverage) AS low_coverage_hours,
       ROUND(100.0 * COUNT(*) FILTER (WHERE low_coverage) / COUNT(*), 1) AS low_coverage_pct
FROM hkt.fact_detector_hour GROUP BY obs_date ORDER BY obs_date;

-- 7. Detectors that report data but are missing from the dimension file
SELECT f.detector_id, COUNT(*) AS detector_hours
FROM hkt.fact_detector_hour f
LEFT JOIN hkt.dim_detector d USING (detector_id)
WHERE d.detector_id IS NULL
GROUP BY f.detector_id ORDER BY detector_hours DESC;

-- 8. Pipeline health: files processed and flagged rows per day
SELECT day, kind, files, rows, flags, detectors_reporting
FROM hkt.pipeline_run ORDER BY day DESC, kind;
