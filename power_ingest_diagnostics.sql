-- Run from the repository root:
-- sqlite3 -bail -ifexists -header -column data/model_dc.db < power_ingest_diagnostics.sql
-- Read-only snapshot of the application and retained inbox. Acknowledged inbox
-- frames expire, so the selected rack's raw samples may no longer be present.
-- Choose the rack/minute with the largest stored average relative to capacity.
CREATE TEMP VIEW diagnostic_target AS
SELECT r.id AS rack_id, r.name AS rack_name, pr.device_id,
       p.capacity_limit_kw, pr.timestamp AS worst_minute,
       pr.watts AS worst_watts
FROM power_readings AS pr
JOIN power_devices AS p ON p.id = pr.device_id
JOIN spaces AS s ON s.power_device_id = p.id
JOIN racks AS r ON r.space_id = s.id
WHERE p.capacity_limit_kw > 0
  AND pr.watts > p.capacity_limit_kw * 1000.0
ORDER BY pr.watts / (p.capacity_limit_kw * 1000.0) DESC,
         pr.timestamp DESC, r.id
LIMIT 1;
PRAGMA query_only = ON;
ATTACH DATABASE 'file:data/power_inbox.db?mode=rw' AS inbox_db;
BEGIN;

SELECT 'Inbox status and capture window' AS diagnostic;
SELECT status, COUNT(*) AS frames, MIN(received_at) AS first_received,
       MAX(received_at) AS last_received
FROM inbox_db.inbox
GROUP BY status;

SELECT 'Retained frame shape and size' AS diagnostic;
SELECT COUNT(*) AS frames, SUM(NOT json_valid(payload)) AS invalid_json,
       MIN(length(payload)) AS shortest_bytes,
       MAX(length(payload)) AS longest_bytes,
       ROUND(AVG(length(payload)), 1) AS average_bytes
FROM inbox_db.inbox;
SELECT COALESCE(json_type(payload, '$.device'), 'missing') AS device_type,
       COALESCE(json_type(payload, '$.metrics'), 'missing') AS metrics_type,
       COUNT(*) AS frames
FROM inbox_db.inbox
WHERE json_valid(payload)
GROUP BY device_type, metrics_type;
SELECT COALESCE(json_extract(payload, '$.type'), '<missing>') AS event_type,
       COUNT(*) AS frames
FROM inbox_db.inbox
WHERE json_valid(payload)
GROUP BY event_type;
SELECT metric.key AS metric, COUNT(*) AS frames
FROM inbox_db.inbox AS i,
     json_each(CASE WHEN json_valid(i.payload) THEN i.payload ELSE '{}' END,
               '$.metrics') AS metric
GROUP BY metric.key;

SELECT 'Raw power values and receipt lag' AS diagnostic;
WITH events AS (
    SELECT CAST(json_extract(payload, '$.device.id') AS INTEGER) AS device_id,
           CAST(json_extract(payload, '$.metrics.active_power_kw') AS REAL) AS power_kw,
           (julianday(received_at) -
            julianday(json_extract(payload, '$.observed_at'))) * 86400 AS lag_seconds
    FROM inbox_db.inbox
    WHERE json_valid(payload)
)
SELECT COUNT(*) AS frames, COUNT(DISTINCT events.device_id) AS devices,
       MIN(power_kw) AS min_kw, MAX(power_kw) AS max_kw,
       SUM(power_kw < 2) AS below_2_kw,
       SUM(power_kw < 5) AS below_5_kw,
       SUM(power_kw > power_devices.capacity_limit_kw) AS over_capacity,
       SUM(power_kw > 30) AS over_30_kw,
       SUM(power_kw > 100) AS over_100_kw,
       SUM(power_kw > 500) AS over_500_kw,
       ROUND(MIN(lag_seconds), 3) AS min_lag_s,
       ROUND(AVG(lag_seconds), 3) AS avg_lag_s,
       ROUND(MAX(lag_seconds), 3) AS max_lag_s,
       SUM(lag_seconds > 1) AS lag_over_1_s,
       SUM(lag_seconds > 30) AS lag_over_30_s
FROM events
LEFT JOIN power_devices ON power_devices.id = events.device_id;

SELECT 'Repeat readings by device and observation time' AS diagnostic;
WITH events AS (
    SELECT json_extract(payload, '$.device.id') AS device_id,
           json_extract(payload, '$.observed_at') AS observed_at,
           json_extract(payload, '$.metrics.active_power_kw') AS power_kw
    FROM inbox_db.inbox
    WHERE json_valid(payload)
), groups AS (
    SELECT COUNT(*) AS frames, MIN(power_kw) AS min_kw, MAX(power_kw) AS max_kw
    FROM events
    WHERE device_id IS NOT NULL AND observed_at IS NOT NULL
    GROUP BY device_id, observed_at
)
SELECT COALESCE(SUM(frames - 1), 0) AS repeated_frames,
       COALESCE(SUM(CASE WHEN min_kw <> max_kw THEN frames - 1 ELSE 0 END), 0)
           AS conflicting_repeats
FROM groups;

SELECT 'Out-of-order first deliveries within each device' AS diagnostic;
-- shortcut: julianday misses sub-millisecond reversals; use a precise parser if those matter.
WITH observations AS (
    SELECT MIN(id) AS id, datacenter_id,
           json_extract(payload, '$.device.id') AS device_id,
           json_extract(payload, '$.observed_at') AS observed_at
    FROM inbox_db.inbox
    WHERE json_valid(payload)
    GROUP BY datacenter_id, device_id, observed_at
), deliveries AS (
    SELECT id, julianday(observed_at) AS observed_time,
           LAG(julianday(observed_at)) OVER (
               PARTITION BY datacenter_id, device_id ORDER BY id
           ) AS previous_observed_time
    FROM observations
    WHERE typeof(device_id) = 'integer' AND typeof(observed_at) = 'text'
      AND julianday(observed_at) IS NOT NULL
)
SELECT COUNT(*) AS backward_steps,
       MIN(id) AS first_backward_inbox_id,
       ROUND(MAX((previous_observed_time - observed_time) * 86400), 3)
           AS largest_step_back_seconds
FROM deliveries
WHERE observed_time < previous_observed_time;

SELECT 'Frames per minute, by receipt and observation time' AS diagnostic;
SELECT 'received' AS time_basis, substr(received_at, 1, 16) AS minute,
       COUNT(*) AS frames
FROM inbox_db.inbox
GROUP BY minute
UNION ALL
SELECT 'observed', substr(json_extract(payload, '$.observed_at'), 1, 16),
       COUNT(*)
FROM inbox_db.inbox
WHERE json_valid(payload)
GROUP BY substr(json_extract(payload, '$.observed_at'), 1, 16)
ORDER BY minute, time_basis;

SELECT 'Distinct observation times per device-minute' AS diagnostic;
WITH events AS (
    SELECT json_extract(payload, '$.device.id') AS device_id,
           substr(json_extract(payload, '$.observed_at'), 1, 16) AS minute,
           json_extract(payload, '$.observed_at') AS observed_at
    FROM inbox_db.inbox
    WHERE json_valid(payload)
), device_minutes AS (
    SELECT COUNT(DISTINCT observed_at) AS samples
    FROM events
    WHERE device_id IS NOT NULL AND observed_at IS NOT NULL
    GROUP BY device_id, minute
)
SELECT samples, COUNT(*) AS device_minutes
FROM device_minutes
GROUP BY samples
ORDER BY samples;

SELECT 'Stored readings and finalized minutes' AS diagnostic;
SELECT COUNT(*) AS readings, COUNT(DISTINCT device_id) AS devices,
       MIN(timestamp) AS first_minute, MAX(timestamp) AS last_minute,
       ROUND(MIN(watts) / 1000.0, 3) AS min_kw,
       ROUND(MAX(watts) / 1000.0, 3) AS max_kw
FROM power_readings;
SELECT COUNT(*) AS finalized_minutes, MIN(timestamp) AS first_minute,
       MAX(timestamp) AS last_minute
FROM finalized_power_minutes;

SELECT 'Stored readings above rated capacity' AS diagnostic;
SELECT COUNT(*) AS readings,
       SUM(power_readings.watts > power_devices.capacity_limit_kw * 1000.0)
           AS over_capacity
FROM power_readings
JOIN power_devices ON power_devices.id = power_readings.device_id;

SELECT 'Device coverage and outliers by stored minute' AS diagnostic;
SELECT pr.timestamp AS minute, COUNT(*) AS devices,
       SUM(pr.watts > p.capacity_limit_kw * 1000.0) AS over_capacity,
       ROUND(AVG(pr.watts) / 1000.0, 3) AS mean_kw,
       ROUND(MAX(pr.watts) / 1000.0, 3) AS max_kw
FROM power_readings AS pr
JOIN power_devices AS p ON p.id = pr.device_id
GROUP BY pr.timestamp
ORDER BY pr.timestamp;

SELECT 'Highest stored device-minute averages' AS diagnostic;
SELECT pr.device_id, pr.timestamp, ROUND(pr.watts / 1000.0, 3) AS power_kw,
       p.capacity_limit_kw
FROM power_readings AS pr
JOIN power_devices AS p ON p.id = pr.device_id
ORDER BY pr.watts DESC
LIMIT 15;

SELECT 'Rack with the highest stored average relative to capacity' AS diagnostic;
SELECT rack_id, rack_name, device_id, worst_minute,
       ROUND(worst_watts / 1000.0, 3) AS worst_mean_kw,
       capacity_limit_kw,
       ROUND(worst_watts / (capacity_limit_kw * 1000.0), 2) AS capacity_multiple
FROM diagnostic_target;

SELECT 'Stored averages around the selected minute' AS diagnostic;
SELECT target.rack_id, pr.timestamp, ROUND(pr.watts / 1000.0, 3) AS power_kw
FROM diagnostic_target AS target
JOIN power_readings AS pr ON pr.device_id = target.device_id
WHERE pr.timestamp >= datetime(target.worst_minute, '-1 minute')
  AND pr.timestamp < datetime(target.worst_minute, '+2 minutes')
ORDER BY pr.timestamp;

SELECT 'Raw frames for the selected rack/minute, if still retained' AS diagnostic;
SELECT target.rack_id, i.id AS inbox_id,
       json_extract(i.payload, '$.observed_at') AS observed_at, i.received_at,
       json_extract(i.payload, '$.metrics.active_power_kw') AS power_kw
FROM inbox_db.inbox AS i
JOIN diagnostic_target AS target ON target.device_id = json_extract(
    CASE WHEN json_valid(i.payload) THEN i.payload ELSE '{}' END, '$.device.id')
WHERE json_valid(i.payload)
  AND strftime('%Y-%m-%d %H:%M', json_extract(i.payload, '$.observed_at'))
      = substr(target.worst_minute, 1, 16)
ORDER BY observed_at, inbox_id;

SELECT 'Selected minute: stored mean versus deduplicated raw mean' AS diagnostic;
-- The 2x-capacity cutoff illustrates the spike's effect; it is not an ingest rule.
WITH samples AS (
    SELECT DISTINCT json_extract(i.payload, '$.observed_at') AS observed_at,
           CAST(json_extract(i.payload, '$.metrics.active_power_kw') AS REAL) AS power_kw
    FROM inbox_db.inbox AS i
    JOIN diagnostic_target AS target ON target.device_id = json_extract(
        CASE WHEN json_valid(i.payload) THEN i.payload ELSE '{}' END, '$.device.id')
    WHERE json_valid(i.payload)
      AND strftime('%Y-%m-%d %H:%M', json_extract(i.payload, '$.observed_at'))
          = substr(target.worst_minute, 1, 16)
)
SELECT target.rack_id, target.worst_minute,
       COUNT(samples.observed_at) AS retained_unique_samples,
       ROUND(AVG(samples.power_kw), 4) AS raw_mean_kw,
       ROUND(AVG(CASE WHEN samples.power_kw <= target.capacity_limit_kw * 2
                      THEN samples.power_kw END), 3) AS mean_without_spike_kw,
       ROUND(target.worst_watts / 1000.0, 4) AS stored_mean_kw
FROM diagnostic_target AS target LEFT JOIN samples ON TRUE
GROUP BY target.rack_id, target.worst_minute, target.capacity_limit_kw,
         target.worst_watts;

COMMIT;
