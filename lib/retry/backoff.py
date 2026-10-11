from __future__ import annotations

import random
import time
from typing import Any, Callable, Iterator, Protocol, runtime_checkable


@runtime_checkable
class StreamSource(Protocol):
    def open(self) -> Iterator[Any]:
        ...


def backoff_delay(
    attempt: int, base_delay: float, max_delay: float, jitter: bool
) -> float:
    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
    if jitter:
        delay = random.uniform(0, delay)
    return delay


def stream_with_retry(
    source: StreamSource,
    on_message: Callable[[Any], None],
    *,
    max_attempts: int | None = None,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retry_on: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError),
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    attempt = 0
    while True:
        try:
            for message in source.open():
                on_message(message)
                attempt = 0  # made progress → reset backoff
            return  # source completed cleanly
        except retry_on:
            attempt += 1
            if max_attempts is not None and attempt >= max_attempts:
                raise
            sleep(backoff_delay(attempt, base_delay, max_delay, jitter))
