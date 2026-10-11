"""Collect BMS events and turn them into durable per-minute rack PDU averages."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.database import SessionLocal
from db.models import FinalizedPowerMinute, PowerDevice, PowerReading
from ingestion.inbox import Inbox, Message, SQLiteInbox
from ingestion.stream_client import DatacenterStream
from lib.logging import get_logger
from lib.retry import backoff_delay


logger = get_logger(__name__)


def parse_power_event(raw: bytes) -> tuple[int, datetime, float] | None:
    """Validate an untrusted frame and return (rack PDU id, UTC time, watts)."""
    try:
        event = json.loads(raw)
        if not isinstance(event, dict):
            return None
        device = event.get("device")
        metrics = event.get("metrics")
        if not isinstance(device, dict) or not isinstance(metrics, dict):
            return None
        device_id = device.get("id")
        kw = metrics.get("active_power_kw")
        observed_at = event.get("observed_at")
        if (type(device_id) is not int or device_id <= 0
                or type(kw) not in (int, float) or kw < 0
                or not isinstance(observed_at, str)):
            return None
        timestamp = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        watts = float(kw) * 1000
        if timestamp.tzinfo is None or not math.isfinite(watts):
            return None
        return device_id, timestamp.astimezone(timezone.utc), watts
    except (ValueError, TypeError, OverflowError, UnicodeError, RecursionError):
        return None


class PowerIngester:
    """Collector process: append each event before reading the next one."""

    def __init__(
        self,
        stream: DatacenterStream,
        token: str,
        data_center_id: int,
        inbox: Inbox | None = None,
    ) -> None:
        self._stream = stream
        self._token = token
        self._data_center_id = data_center_id
        self._inbox = inbox or SQLiteInbox()

    def run(self) -> None:
        attempts = 0
        while True:
            try:
                for event in self._stream.subscribe(self._token, self._data_center_id):
                    payload = (json.dumps(event).encode() if isinstance(event, dict)
                               else event.encode() if isinstance(event, str) else event)
                    write_attempts = 0
                    while True:
                        try:
                            self._inbox.append(self._data_center_id, payload)
                            break
                        except sqlite3.OperationalError as exc:
                            write_attempts = min(write_attempts + 1, 6)
                            logger.warning("inbox append failed; retrying: %s", exc)
                            time.sleep(backoff_delay(write_attempts, 1, 30, True))
                    attempts = 0
                logger.warning("BMS stream ended; reconnecting")
            except (ConnectionError, TimeoutError, OSError) as exc:
                logger.warning("BMS stream disconnected; reconnecting: %s", exc)
            attempts = min(attempts + 1, 6)
            time.sleep(backoff_delay(attempts, 1, 30, True))


class PowerWorker:
    """One worker per datacenter; rebuilds open minutes from unacked frames."""

    def __init__(
        self,
        data_center_id: int,
        inbox: Inbox | None = None,
        session_factory: Callable[[], Session] = SessionLocal,
        *,
        lateness_seconds: float = 30,
        retention_seconds: float = 3600,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._data_center_id = data_center_id
        self._inbox = inbox or SQLiteInbox()
        self._session_factory = session_factory
        self._lateness = timedelta(seconds=lateness_seconds)
        self._retention = timedelta(seconds=retention_seconds)
        self._clock = clock
        self._cursor = 0
        self._devices: set[int] | None = None
        self._samples: dict[datetime, dict[tuple[int, datetime], float]] = {}
        self._message_ids: dict[datetime, list[int]] = {}
        self._last_cleanup = self._clock()
        self.stats: Counter[str] = Counter()

    def run(self) -> None:
        while True:
            count = self.process_once()
            if not count:
                time.sleep(0.5)

    def process_once(self) -> int:
        if self._devices is None:
            with self._session_factory() as session:
                FinalizedPowerMinute.__table__.create(bind=session.get_bind(), checkfirst=True)
                self._devices = set(session.scalars(select(PowerDevice.id).where(
                    PowerDevice.datacenter_id == self._data_center_id,
                    PowerDevice.device_type == "rack_pdu",
                )))
            if not self._devices:
                raise RuntimeError("no rack PDUs for this datacenter; run make seed first")

        messages = self._inbox.claim(self._data_center_id, self._cursor, 500)
        with self._session_factory() as session:
            for message in messages:
                self._cursor = message.id
                self._handle(message, session)

        now = self._clock()
        # Append order bounds the receipt-time watermark while replaying a backlog.
        self._finalize_ready(messages[-1].received_at if messages else now)
        if now - self._last_cleanup >= timedelta(minutes=1):
            deleted = self._inbox.cleanup(now - self._retention)
            logger.info("power inbox cleanup: %s removed; counters=%s", deleted, dict(self.stats))
            self._last_cleanup = now
        return len(messages)

    def _reject(self, message: Message, reason: str) -> None:
        self.stats[reason] += 1
        logger.debug("dropping inbox message %s: %s", message.id, reason)
        self._inbox.ack([message.id])

    def _handle(self, message: Message, session: Session) -> None:
        sample = parse_power_event(message.payload)
        if sample is None:
            return self._reject(message, "invalid")
        device_id, timestamp, watts = sample
        if device_id not in self._devices:
            return self._reject(message, "unknown_device")
        if timestamp > message.received_at + timedelta(minutes=1):
            return self._reject(message, "future_timestamp")
        minute = timestamp.replace(second=0, microsecond=0)
        deadline = minute + timedelta(minutes=1) + self._lateness
        if message.received_at >= deadline:
            return self._reject(message, "late")
        minute_key = minute.replace(tzinfo=None)
        if session.get(FinalizedPowerMinute, (self._data_center_id, minute_key)):
            return self._reject(message, "finalized")
        samples = self._samples.setdefault(minute, {})
        key = (device_id, timestamp)
        if key in samples:
            return self._reject(message, "duplicate")
        samples[key] = watts
        self._message_ids.setdefault(minute, []).append(message.id)
        self.stats["accepted"] += 1

    def _finalize_ready(self, watermark: datetime) -> None:
        for minute in sorted(self._samples):
            if minute + timedelta(minutes=1) + self._lateness > watermark:
                break
            ids = self._message_ids[minute]
            minute_key = minute.replace(tzinfo=None)
            with self._session_factory() as session:
                if not session.get(FinalizedPowerMinute, (self._data_center_id, minute_key)):
                    totals: dict[int, tuple[float, int]] = {}
                    for (device_id, _timestamp), watts in self._samples[minute].items():
                        total, count = totals.get(device_id, (0.0, 0))
                        totals[device_id] = (total + watts, count + 1)
                    for device_id, (total, count) in totals.items():
                        session.merge(PowerReading(
                            device_id=device_id, timestamp=minute_key, watts=total / count,
                        ))
                    session.add(FinalizedPowerMinute(
                        datacenter_id=self._data_center_id, timestamp=minute_key,
                    ))
                    session.commit()
            self._inbox.ack(ids)
            self.stats["finalized_minutes"] += 1
            logger.info("finalized power minute %s: %s samples", minute.isoformat(), len(ids))
            del self._samples[minute]
            del self._message_ids[minute]
