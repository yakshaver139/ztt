from __future__ import annotations

from typing import Any, Callable

from sqlalchemy.orm import Session

from db.database import SessionLocal

EventHandler = Callable[[dict, Session], Any]


class EventProcessor:
    def __init__(self, session_factory: Callable[[], Session] = SessionLocal) -> None:
        self._handlers: dict[str, EventHandler] = {}
        self._session_factory = session_factory

    def register(self, event_type: str, handler: EventHandler) -> None:
        self._handlers[event_type] = handler

    def process(self, event: dict) -> None:
        handler = self._handlers.get(event.get("type"))
        if handler is None:
            return

        session = self._session_factory()
        try:
            result = handler(event, session)
            if result is not None:
                session.add(result)
            session.commit()
        except Exception:
            pass
        finally:
            session.close()

    def process_all(self, events: Any) -> None:
        for event in events:
            self.process(event)
