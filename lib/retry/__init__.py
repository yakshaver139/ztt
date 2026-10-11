"""Reconnect-with-backoff utilities for streaming ingesters.

The canonical way to consume an unreliable stream in this codebase: implement the
:class:`StreamSource` protocol and hand it to :func:`stream_with_retry`, which
reconnects with exponential backoff when the connection drops. See
``ingestion/weather_ingester.py`` for a worked example.
"""
from lib.retry.backoff import StreamSource, backoff_delay, stream_with_retry

__all__ = ["StreamSource", "stream_with_retry", "backoff_delay"]
