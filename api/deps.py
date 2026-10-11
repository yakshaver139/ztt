"""FastAPI dependencies."""
from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy.orm import Session

from db.database import SessionLocal


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
