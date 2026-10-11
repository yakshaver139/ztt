from __future__ import annotations

from datetime import datetime
from typing import Callable, Iterator

from sqlalchemy.orm import Session

from db.database import SessionLocal
from db.models import WeatherReading
from lib.retry import stream_with_retry


class WeatherIngester:
    def __init__(
        self,
        datacenter_id: int,
        latitude: float,
        longitude: float,
        session_factory: Callable[[], Session] = SessionLocal,
    ) -> None:
        self._datacenter_id = datacenter_id
        self._lat = latitude
        self._lon = longitude
        self._session_factory = session_factory

    # ── StreamSource protocol ────────────────────────────────────────────────
    def open(self) -> Iterator[dict]:
        raise NotImplementedError("Connect to the weather source and yield observations.")

    # ── driver ───────────────────────────────────────────────────────────────
    def run(self) -> None:
        stream_with_retry(self, self._store, base_delay=2.0, max_delay=120.0)

    def _store(self, obs: dict) -> None:
        session = self._session_factory()
        try:
            session.merge(
                WeatherReading(
                    datacenter_id=self._datacenter_id,
                    timestamp=datetime.fromisoformat(obs["timestamp"].replace("Z", "+00:00")),
                    temp_c=obs.get("temp_c"),
                    humidity_pct=obs.get("humidity_pct"),
                    wind_mps=obs.get("wind_mps"),
                )
            )
            session.commit()
        finally:
            session.close()
