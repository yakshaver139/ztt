"""Generic event processing: map event types to handlers and persist results.

WARNING: this is written in the same naive style as the rest of the ingestion
path — it is not a model of good practice. See :mod:`lib.events.processor` for
the known rough edges.
"""
from lib.events.processor import EventHandler, EventProcessor

__all__ = ["EventProcessor", "EventHandler"]
