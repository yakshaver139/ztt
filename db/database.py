from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# SQLite only for the interview project (the real dc_demo runs Postgres).
# Override with DATABASE_URL if you want to point elsewhere.
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "model_dc.db"


def get_database_url() -> str:
    if database_url := os.getenv("DATABASE_URL"):
        return database_url
    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{DEFAULT_DB_PATH}"


DATABASE_URL = get_database_url()

# check_same_thread=False lets the FastAPI dev server share the connection across
# threads; harmless for a single-user demo DB.
engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(
    bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
)
