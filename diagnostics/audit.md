What this repo does: It models one demo datacenter, ingests BMS power events into a SQLite inbox, and stores per-minute rack power averages. A FastAPI app serves utilization reports and a dashboard. I assumed the documented load: 1,000 rack PDUs, one collector and worker, roughly 200 incoming frames per second, and a few dashboard viewers.

## Must fix

1. **Cleanup can make the worker skip newly inserted events** (`ingestion/inbox.py:40-42`, `ingestion/inbox.py:74-85`, `ingestion/power_ingester.py:142-145`)
   - **What this is:** The worker remembers the last inbox ID it read and asks for higher IDs next time.
   - **Problem:** The ID is `INTEGER PRIMARY KEY` without `AUTOINCREMENT`. I reproduced this: after cleanup empties the inbox, the next insert gets ID 1 again, so a running worker whose cursor is already 1 never claims it.
   - **Fix:** Make inbox IDs non-reusable, including a migration for existing inbox files, and add a cleanup-then-append regression test.
   - **If we skip it:** After cleanup, telemetry with reused IDs remains pending until the worker restarts, while the worker appears healthy.

2. **A live but silent WebSocket leaves ingestion stalled** (`ingestion/stream_client.py:40-45`, `jobs/run_power_ingest.py:69-74`)
   - **What this is:** The collector waits for an authentication reply and then for telemetry frames.
   - **Problem:** `open_timeout` covers connection setup, but `ws.recv()` and iteration have no message deadline. A server that keeps the socket open and responds to pings without sending an auth reply or telemetry leaves the collector alive, so the supervisor does nothing.
   - **Fix:** Set a deadline for the auth reply and each expected telemetry frame; log and reconnect when it expires.
   - **If we skip it:** The dashboard can show an aging snapshot for hours without an ingestion restart or a clear failure.

3. **One valid JSON number disconnects the collector** (`ingestion/stream_client.py:45-53`)
   - **What this is:** The stream client parses each WebSocket frame before the collector stores it.
   - **Problem:** A frame containing `123` parses successfully, then `"metrics" in event` raises `TypeError`. I reproduced the exception; the collector exits and the supervisor reconnects, losing frames during the gap because the BMS has no replay.
   - **Fix:** Check `isinstance(event, dict)` before looking for `metrics`, and test a valid JSON value that is not an object.
   - **If we skip it:** One malformed publisher frame causes avoidable disconnects and gaps in stored power data.

4. **“Current” rack and client load can be hours old** (`api/repositories/power_repository.py:52-58`, `api/services/rack_service.py:28-32`, `api/services/client_service.py:39-47`)
   - **What this is:** Rack and client endpoints use each rack PDU's latest reading as its current load.
   - **Problem:** If a rack stops reporting at 10:00, a request at 13:00 still includes its 10:00 load. Client totals also mix readings from different minutes, while the dashboard correctly uses one finalized minute.
   - **Fix:** Read one chosen finalized minute for all racks and report missing devices as missing instead of reusing their old readings.
   - **If we skip it:** Clients can see a plausible but false current load and utilization percentage during telemetry gaps.

5. **The utilization endpoint does not finish at the current demo size** (`api/services/rollup_service.py:24-35`, `api/services/rollup_service.py:52-59`)
   - **What this is:** `/api/datacenters/{id}/utilization` computes peak and mean loads for 1,126 power devices.
   - **Problem:** For every device it fetches all datacenter readings again. With the current 4,000-reading snapshot, I stopped a read-only call after 15 seconds and 960 SQL statements; it had not returned.
   - **Fix:** Read the readings once and aggregate each rack PDU's values into its upstream devices in one pass.
   - **If we skip it:** This endpoint gets slower with every finalized minute and is already unusable on the supplied snapshot.

## Should fix

6. **Expired and future contracts count as current capacity** (`api/services/client_service.py:20-27`, `db/models.py:273-282`)
   - **What this is:** Client utilization divides current rack load by the sum of its contracts.
   - **Problem:** The query ignores `start_date` and `end_date`. A client with a 100 kW active contract and a 100 kW expired one is reported against 200 kW, halving the shown utilization.
   - **Fix:** Sum only contracts active at the request time. Return an unavailable percentage when no active capacity exists.
   - **If we skip it:** Contract changes can silently make the client utilization figure wrong.

7. **PATCH cannot clear a nullable suite capacity** (`api/routers/suite.py:22-27`, `api/services/suite_service.py:19-21`, `api/schemas.py:53-56`)
   - **What this is:** The suite PATCH endpoint accepts `sla_capacity_kw`, which the database allows to be null.
   - **Problem:** Sending `{"sla_capacity_kw": null}` is treated like an omitted field because the service removes every `None` value. The old capacity remains.
   - **Fix:** Pass only fields actually supplied with `model_dump(exclude_unset=True)`, allow null for `sla_capacity_kw`, and reject null for the required `name`.
   - **If we skip it:** API clients cannot remove an SLA ceiling even though the schema says they can.

8. **The API labels minutes as hours** (`api/services/rollup_service.py:64-80`, `api/schemas.py:7-19`)
   - **What this is:** `hours_over_80pct` is returned in each device utilization record.
   - **Problem:** Power readings are minute averages, and the code counts readings above 80% capacity without converting the count. Sixty high minutes are reported as 60 hours.
   - **Fix:** Rename the field to `minutes_over_80pct`, or divide by 60 and return a fractional hour value.
   - **If we skip it:** API consumers overstate time above 80% capacity by a factor of 60.

9. **Renaming a suite to an existing name returns a server error** (`api/repositories/suite_repository.py:24-36`, `db/models.py:85-94`)
   - **What this is:** The suite PATCH endpoint writes a new name and commits it.
   - **Problem:** Suite names are unique within a datacenter, but the update does not handle a duplicate. Renaming Hall A to Hall B raises a database error and returns HTTP 500 instead of a useful client error.
   - **Fix:** Catch the unique-constraint error, roll back, and return HTTP 409; test the duplicate-name request.
   - **If we skip it:** A normal invalid edit looks like a server outage to API clients.

## Nice to have

10. **Rack and client list requests issue a query per rack** (`api/services/rack_service.py:22-31`, `api/services/client_service.py:39-47`)
   - **What this is:** The list endpoints build each rack summary by loading its relations and latest reading separately.
   - **Problem:** A request for one seeded client with 256 racks executed 770–771 SQL statements in my read-only check. It was fast on local SQLite, but extra viewers multiply that work.
   - **Fix:** Load rack relations together and fetch the requested devices' readings in one query.
   - **If we skip it:** These endpoints waste database work and will slow down as request volume grows.

11. **Every dashboard refresh takes an inbox write lock** (`api/routers/dashboard.py:59-63`, `ingestion/inbox.py:34-56`)
    - **What this is:** The dashboard polls its snapshot every five seconds and reads inbox status.
    - **Problem:** Each request constructs `SQLiteInbox`, whose constructor starts an immediate write transaction and checks schema and indexes before the read. That competes with the collector's inbox writes.
    - **Fix:** Initialize the inbox once per API process and reuse it for read-only summaries.
    - **If we skip it:** Polling adds avoidable SQLite lock contention while telemetry is arriving.

12. **Unused scaffolding adds code and an unused dependency** (`ingestion/weather_ingester.py:13-48`, `lib/events/processor.py:12-38`, `ingestion/stream_client.py:63-69`, `pyproject.toml:10-14`)
    - **What this is:** The weather ingester, event processor, and `parse_event` have no callers; `numpy` has no imports in the repo.
    - **Problem:** `WeatherIngester.run()` always reaches `NotImplementedError`, while the unused event processor silently swallows handler errors. They still add paths to read and maintain.
    - **Fix:** Delete the unused helpers and the `numpy` dependency until there is a real caller.
    - **If we skip it:** Future changes may rely on broken scaffolding, and installs carry an unnecessary package.

Verdict: Fix inbox ID reuse first, then the silent stream and slow utilization endpoint.

Lean: -100 lines, -1 dependency possible.

Not checked: A live BMS connection, a full multi-process ingest run, or browser behavior under load. The project's 19 `unittest` tests passed in `.venv`; the default Python lacks SQLAlchemy and `.venv` lacks pytest.
