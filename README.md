# model-dc

A deliberately simplified **datacenter digital twin**. It models a datacenter's
physical and electrical hierarchy, the colocation tenants who rent racks, and
live power telemetry ingested from a streaming publisher.

## The model at a glance

```
Owner                         the company that owns the datacenter
  └─ Datacenter
       └─ Suite                a data hall (has an SLA power ceiling)
            ├─ Space           a fixed floor position (row, position, x/y)
            │    └─ Rack       occupies a space; rented by a Client
            └─ power tree:  MSB ──▶ PDU ──▶ Rack PDU
                                                 └─ metered here (PowerReading)

Client                        a colocation tenant (rents racks, holds contracts)
```

- **Power hierarchy** is three levels — main switchboard (MSB) → PDU → rack PDU —
  wired with single feeds via `power_connections`.
- **Only rack PDUs are metered.** A PDU's or MSB's load is derived by summing its
  descendants' readings.
- Every **Space** has a designated rack PDU and floor coordinates, so a visual
  floor layer can be added later.
- **Readings arrive live.** The ingester subscribes to the telemetry publisher
  and writes per-minute averages into `power_readings`.

## Layout

```
db/         database.py (SQLite), models.py (the schema above)
api/        FastAPI app — routers / services / repositories (thin layering)
ingestion/  telemetry stream client + power/weather ingesters
jobs/       runnable entrypoints: seed_datacenter.py, run_power_ingest.py
lib/        shared helpers (e.g. lib/retry and lib/logging)
```

Packages sit at the repo root; imports are top-level (`db`, `api`, `ingestion`,
`jobs`, `lib`). Run Python from the repo root.

## Logging

Use the shared logger in any module:

```python
from lib.logging import get_logger

logger = get_logger(__name__)
logger.info("Application started")
```

Logs go to stderr at INFO level by default. If the host application has already
configured Python logging, that configuration takes precedence.

## Quick start
Set up the model data center. This will populate racks, power devices, suites, etc.
Make sure you do this before running data ingest or fiddling with the API.

```bash
pip install -e .          # core deps (SQLAlchemy, numpy, pydantic)
make seed                 # creates data/model_dc.db with the datacenter hierarchy
```

Ingest live telemetry (needs the publisher running and an issued token):

```bash
pip install -e '.[ingest]'
export BMS_TOKEN=<token>  # Ask interviewer for token
export BMS_WS_URL=<url>   # Ask interviewer for token
export BMS_DATACENTER_ID=1
make ingest
```

`make ingest` supervises two processes. The stream client yields only parsed events
with metrics; the collector commits each as JSON to `data/power_inbox.db`. The worker
claims pending or unfinished events, validates them, deduplicates by device and
timestamp, and writes one average per device/minute to `data/model_dc.db`.
Minutes close 30 seconds after their end by default. Change this with
`BMS_ALLOWED_LATENESS_SECONDS`; acknowledged inbox events are retained for one
hour by default (`BMS_INBOX_RETENTION_HOURS`). A completed minute and its
readings commit together. Only then are its stored events acknowledged, so a worker
restart can replay unfinished work without writing the minute twice.

The worker logs rejected frames by reason, minute completions, and periodic
counters. Invalid, unknown-device, future-dated (over one minute), duplicate,
and late frames are acknowledged without contributing to an average. A frame
received after a minute's deadline is late even if the worker was backlogged.
The BMS has no replay: frames sent while the collector is disconnected, or lost
before the inbox commit, cannot be recovered. Run `make clean` only when both
processes are stopped and you intend to discard all stored telemetry. `make seed`
recreates the application tables but does not clear the inbox.

Add the API server when needed:

```bash
pip install -e '.[api]'
make run-api              # http://localhost:8000/docs
```

The application database and inbox are separate SQLite files under `data/`
(git-ignored). `make clean` deletes both, and `make seed` rebuilds the
application database.
