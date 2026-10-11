.PHONY: help install seed ingest run-api clean

help:
	@echo "Targets:"
	@echo "  install   pip install -e . (add [api] for the server, [ingest] for the ingester)"
	@echo "  seed      build the schema + datacenter fixtures (hierarchy; no readings)"
	@echo "  ingest    run the power ingester against the telemetry publisher (needs BMS_TOKEN)"
	@echo "  run-api   uvicorn dev server on :8000 (needs the api extra)"
	@echo "  clean     delete the local SQLite database"

install:
	pip install -e .

# Run Python from the repo root; module paths are top-level (db, jobs, api).
seed:
	python -m jobs.seed_datacenter

ingest:
	python -m jobs.run_power_ingest

run-api:
	python -m uvicorn api.main:app --reload

clean:
	rm -f data/model_dc.db
	rm -f data/power_inbox.db
