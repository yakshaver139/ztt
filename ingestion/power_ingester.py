"""Power telemetry ingester — YOUR IMPLEMENTATION.

Goal: consume the datacenter's 5-second power telemetry and turn it into
trustworthy per-minute average readings in ``power_readings`` (one row per
device per minute).

Provided for you to build on:
  - ``ingestion.stream_client.RealDatacenterStream`` — connects to the publisher
    and yields telemetry events; ``parse_event`` turns one into a ``Sample``
    (device_id, observed_at, watts).
  - ``lib.retry`` — reconnect-with-backoff; ``ingestion.weather_ingester`` shows
    how an ingester uses it.
  - ``db.models.PowerReading`` and ``db.database.SessionLocal`` — persistence.

The live feed is not clean. Part of the task is characterising how it misbehaves
and making the pipeline robust to it. How you structure the solution is up to you.
"""
from __future__ import annotations

from typing import Callable

from sqlalchemy.orm import Session

from db.database import SessionLocal
from ingestion.stream_client import DatacenterStream
from lib.logging import get_logger


logger = get_logger(__name__)


class PowerIngester:
    def __init__(
        self,
        stream: DatacenterStream,
        token: str,
        data_center_id: int,
        session_factory: Callable[[], Session] = SessionLocal,
    ) -> None:
        self._stream = stream
        self._token = token
        self._data_center_id = data_center_id
        self._session_factory = session_factory

    def run(self) -> None:
        """Subscribe to the stream, aggregate the 5-second readings into
        per-minute averages, and persist them to ``power_readings`` — robustly."""
        for event in self._stream.subscribe(self._token, self._data_center_id):
          logger.info(f"received {event}")
