"""FastAPI application entrypoint.

Run from the repo root::

    python -m uvicorn api.main:app --reload

Swagger UI at http://localhost:8000/docs.
"""
from __future__ import annotations

from fastapi import FastAPI

from api.routers import client, rack, rollup, suite

app = FastAPI(title="dc-interview", version="0.1.0")
app.include_router(rollup.router)
app.include_router(client.router)
app.include_router(suite.router)
app.include_router(rack.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
