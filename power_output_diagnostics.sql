-- Run from the repository root:
-- sqlite3 -bail -header -column :memory: < power_output_diagnostics.sql
-- The captures have no overlapping minutes; rates describe separate runs.
-- Output alone cannot verify deduplication or isolate individual fixes.
ATTACH DATABASE 'file:data/model_dc_6e63013.db?mode=ro' AS before;
ATTACH DATABASE 'file:data/model_dc.db?mode=ro' AS after;

CREATE TEMP VIEW modeled_readings AS
SELECT 'before' AS run, r.device_id, r.timestamp, r.watts,
       d.capacity_limit_kw * 1000.0 AS capacity_watts
FROM before.power_readings AS r
LEFT JOIN before.power_devices AS d ON d.id = r.device_id
UNION ALL
SELECT 'after', r.device_id, r.timestamp, r.watts,
       d.capacity_limit_kw * 1000.0
FROM after.power_readings AS r
LEFT JOIN after.power_devices AS d ON d.id = r.device_id;

PRAGMA query_only = ON;

SELECT 'Capture windows and modeled coverage' AS diagnostic;
SELECT run, MIN(timestamp) AS first_minute, MAX(timestamp) AS last_minute,
       COUNT(DISTINCT timestamp) AS minutes, COUNT(DISTINCT device_id) AS devices,
       COUNT(*) AS device_minutes
FROM modeled_readings
GROUP BY run
ORDER BY CASE run WHEN 'before' THEN 0 ELSE 1 END;

SELECT 'Modeled averages relative to device capacity' AS diagnostic;
SELECT run, COUNT(*) AS device_minutes,
       COUNT(*) FILTER (WHERE capacity_watts IS NULL OR capacity_watts <= 0)
           AS missing_capacity,
       COUNT(*) FILTER (WHERE watts > capacity_watts) AS over_capacity,
       ROUND(100.0 * COUNT(*) FILTER (WHERE watts > capacity_watts)
             / NULLIF(COUNT(*), 0), 2) AS over_capacity_pct,
       COUNT(*) FILTER (WHERE watts > 2 * capacity_watts) AS over_2x_capacity,
       ROUND(MAX(watts / NULLIF(capacity_watts, 0)), 2) AS max_capacity_multiple,
       ROUND(MAX(watts) / 1000.0, 2) AS max_kw
FROM modeled_readings
GROUP BY run
ORDER BY CASE run WHEN 'before' THEN 0 ELSE 1 END;

SELECT 'Capacity breaches by modeled minute' AS diagnostic;
SELECT run, timestamp AS minute, COUNT(*) AS devices,
       COUNT(*) FILTER (WHERE watts > capacity_watts) AS over_capacity,
       COUNT(*) FILTER (WHERE watts > 2 * capacity_watts) AS over_2x_capacity,
       ROUND(MAX(watts / NULLIF(capacity_watts, 0)), 2) AS max_capacity_multiple
FROM modeled_readings
GROUP BY run, timestamp
ORDER BY CASE run WHEN 'before' THEN 0 ELSE 1 END, timestamp;

SELECT 'Fixed run: recorded accepted samples per device-minute' AS diagnostic;
SELECT c.timestamp AS minute, SUM(j.value) AS devices_with_samples,
       SUM(CAST(j.key AS INTEGER) * j.value) AS accepted_samples,
       ROUND(1.0 * SUM(CAST(j.key AS INTEGER) * j.value) / SUM(j.value), 2)
           AS mean_samples_per_device,
       MIN(CAST(j.key AS INTEGER)) AS min_samples_per_device,
       MAX(CAST(j.key AS INTEGER)) AS max_samples_per_device
FROM after.power_minute_coverage AS c
JOIN json_each(c.sample_counts) AS j
GROUP BY c.timestamp
ORDER BY c.timestamp;
