from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Iterator, Protocol, runtime_checkable
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from lib.logging import get_logger

Event = dict

logger = get_logger(__name__)

class StreamAuthError(Exception):
    """Non-retryable: the server rejected the token / data center (close 1008)."""


@runtime_checkable
class DatacenterStream(Protocol):
    """Opens a stream for one datacenter and yields telemetry events until the
    connection ends or drops (raising a retryable error)."""

    def subscribe(self, token: str, data_center_id: int) -> Iterator[Event]:
        ...


class RealDatacenterStream:
    def __init__(self, base_url: str | None = None, *, open_timeout: float = 10.0) -> None:
        self._base_url = (base_url or os.getenv("BMS_WS_URL", "ws://localhost:8000")).rstrip("/")
        self._open_timeout = open_timeout

    def subscribe(self, token: str, data_center_id: int) -> Iterator[Event]:

        url = f"{self._base_url}/ws/data-centers/{data_center_id}"
        try:
            with connect(url, open_timeout=self._open_timeout) as ws:
                ws.send(json.dumps({"type": "authenticate", "token": token}))
                ack = json.loads(ws.recv())
                if ack.get("type") != "authenticated":
                    raise StreamAuthError(f"authentication failed: {ack!r}")
                for raw in ws:
                    event = json.loads(raw)
                    logger.info(parse_event(event))
                    if "metrics" in event:
                        yield event
        except ConnectionClosed as exc:
            # 1008 = policy violation (bad token / already connected): don't retry.
            if getattr(exc, "code", None) == 1008:
                raise StreamAuthError(str(exc)) from exc
            raise ConnectionError(f"stream closed: {exc}") from exc
        except (OSError, TimeoutError) as exc:
            raise ConnectionError(f"connection failed: {exc}") from exc


def parse_event(event: Event) -> tuple[int, datetime, float | None] | None:
    if "metrics" not in event or "device" not in event:
        return None
    device_id = event["device"]["id"]
    observed_at = datetime.fromisoformat(event["observed_at"].replace("Z", "+00:00"))
    kw = event["metrics"].get("active_power_kw")
    watts = None if kw is None else kw * 1000.0
    return device_id, observed_at, watts