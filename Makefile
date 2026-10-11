.PHONY: help install seed ingest kill-worker bms-process check-ingest inbox-status inbox-backlog power-status power-minutes power-readings device-readings rack-readings run-api clean

help:
	@echo "Targets:"
	@echo "  install   pip install -e . (add [api] for the server, [ingest] for the ingester)"
	@echo "  seed      build the schema + datacenter fixtures (hierarchy; no readings)"
	@echo "  ingest    run the power ingester against the telemetry publisher (needs BMS_TOKEN)"
	@echo "  kill-worker  kill the supervised worker to test automatic restart"
	@echo "  bms-process  show the process connected to BMS_WS_URL"
	@echo "  check-ingest  run all SQLite checks"
	@echo "  inbox-status  count messages by status and show the latest receipt"
	@echo "  inbox-backlog  show the oldest unacknowledged message"
	@echo "  power-status  count readings and finalized minutes"
	@echo "  power-minutes  count devices in the five latest minutes"
	@echo "  power-readings  show the five latest readings"
	@echo "  device-readings  show the five latest readings for DEVICE_ID"
	@echo "  rack-readings  show up to 100 highest readings and capacity for RACK_ID"
	@echo "  run-api   uvicorn dev server on :8000 (needs the api extra)"
	@echo "  clean     delete the local SQLite database"

install:
	pip install -e .

# Run Python from the repo root; module paths are top-level (db, jobs, api).
seed:
	python -m jobs.seed_datacenter

ingest:
	python -m jobs.run_power_ingest

kill-worker:
	@set -- $$(pgrep -f ' -m jobs[.]run_power_ingest$$'); \
	if [ "$$#" -ne 1 ]; then echo "Expected one running ingest supervisor; found $$#" >&2; exit 1; fi; \
	kill -URG "$$1"

bms-process:
	@url=$${BMS_WS_URL:?Set BMS_WS_URL to the BMS base URL}; \
	endpoint=$${url#*://}; endpoint=$${endpoint%%/*}; \
	output=$$(lsof -nP -iTCP@$$endpoint 2>&1); status=$$?; \
	if [ $$status -eq 0 ]; then printf '%s\n' "$$output"; \
	elif [ -z "$$output" ]; then echo "No local process connected to $$endpoint"; \
	else printf '%s\n' "$$output" >&2; exit $$status; fi

check-ingest: inbox-status inbox-backlog power-status power-minutes power-readings

inbox-status:
	@echo "Inbox (data/power_inbox.db):"
	@sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/power_inbox.db "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'pending') AS pending, COUNT(*) FILTER (WHERE status = 'in_flight') AS in_flight, COUNT(*) FILTER (WHERE status = 'acked') AS acked, MAX(received_at) AS latest_received_at FROM inbox;"

inbox-backlog:
	@sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/power_inbox.db "SELECT MIN(received_at) AS oldest_unacked_received_at FROM inbox WHERE status IN ('pending', 'in_flight');"

power-status:
	@echo "Application (data/model_dc.db):"
	@sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/model_dc.db "SELECT 'power_readings' AS table_name, COUNT(*) AS rows, MAX(timestamp) AS latest_minute FROM power_readings UNION ALL SELECT 'finalized_power_minutes', COUNT(*), MAX(timestamp) FROM finalized_power_minutes;"

power-minutes:
	@sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/model_dc.db "SELECT timestamp AS minute, COUNT(*) AS devices FROM power_readings GROUP BY timestamp ORDER BY timestamp DESC LIMIT 5;"

power-readings:
	@sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/model_dc.db "SELECT device_id, timestamp, watts FROM power_readings ORDER BY timestamp DESC LIMIT 5;"

device-readings:
	@id=$${DEVICE_ID:-}; \
	case "$$id" in ''|0*|*[!0-9]*) echo "Usage: make device-readings DEVICE_ID=<positive integer>" >&2; exit 1;; esac; \
	sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/model_dc.db "SELECT device_id, timestamp, watts FROM power_readings WHERE device_id = $$id ORDER BY timestamp DESC LIMIT 5;"

rack-readings:
	@id=$${RACK_ID:-}; \
	case "$$id" in ''|0*|*[!0-9]*) echo "Usage: make rack-readings RACK_ID=<positive integer>" >&2; exit 1;; esac; \
	sqlite3 -ifexists -cmd 'PRAGMA query_only=ON' -header -column data/model_dc.db "SELECT r.id AS rack_id, r.name AS rack_name, p.id AS device_id, pr.timestamp, ROUND(pr.watts / 1000.0, 3) AS power_kw, p.capacity_limit_kw FROM racks r JOIN spaces s ON s.id = r.space_id JOIN power_devices p ON p.id = s.power_device_id JOIN power_readings pr ON pr.device_id = p.id WHERE r.id = $$id ORDER BY pr.watts DESC LIMIT 100;"

make diagnostics:
	sqlite3 -bail -ifexists -header -column data/model_dc.db < power_ingest_diagnostics.sql

run-api:
	python -m uvicorn api.main:app --reload

clean:
	rm -f data/model_dc.db
	rm -f data/power_inbox.db
