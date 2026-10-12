"""Durable raw-message inbox shared by the collector and worker processes."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Protocol


DEFAULT_INBOX_PATH = Path(__file__).resolve().parents[1] / "data" / "power_inbox.db"


@dataclass(frozen=True)
class Message:
    id: int
    payload: bytes
    received_at: datetime


class Inbox(Protocol):
    def append(self, datacenter_id: int, payload: bytes) -> None: ...

    def claim(self, datacenter_id: int, after_id: int, limit: int) -> list[Message]: ...

    def ack(self, ids: Iterable[int], result: str = "accepted") -> None: ...

    def cleanup(self, before: datetime) -> int: ...


class SQLiteInbox:
    def __init__(self, path: Path = DEFAULT_INBOX_PATH) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("BEGIN IMMEDIATE")
            db.execute("""
                CREATE TABLE IF NOT EXISTS inbox (
                    id INTEGER PRIMARY KEY,
                    datacenter_id INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    received_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    acked_at TEXT,
                    result TEXT
                )
            """)
            if "result" not in {row[1] for row in db.execute("PRAGMA table_info(inbox)")}:
                db.execute("ALTER TABLE inbox ADD COLUMN result TEXT")
            db.execute(
                "CREATE INDEX IF NOT EXISTS ix_inbox_claim ON inbox (datacenter_id, status, id)"
            )
            db.execute("CREATE INDEX IF NOT EXISTS ix_inbox_cleanup ON inbox (status, acked_at)")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def append(self, datacenter_id: int, payload: bytes) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO inbox (datacenter_id, payload, received_at) VALUES (?, ?, ?)",
                (datacenter_id, payload, datetime.now(timezone.utc).isoformat()),
            )

    def claim(self, datacenter_id: int, after_id: int, limit: int = 500) -> list[Message]:
        # shortcut: one worker per datacenter; add leases before running parallel workers.
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                """SELECT id, payload, received_at FROM inbox
                   WHERE datacenter_id = ? AND status = 'pending' AND id > ?
                   UNION ALL
                   SELECT id, payload, received_at FROM inbox
                   WHERE datacenter_id = ? AND status = 'in_flight' AND id > ?
                   ORDER BY id LIMIT ?""",
                (datacenter_id, after_id, datacenter_id, after_id, limit),
            ).fetchall()
            db.executemany(
                "UPDATE inbox SET status = 'in_flight' WHERE id = ?", ((r[0],) for r in rows)
            )
        return [Message(row[0], row[1], datetime.fromisoformat(row[2])) for row in rows]

    def ack(self, ids: Iterable[int], result: str = "accepted") -> None:
        ids = list(ids)
        if not ids:
            return
        with self._connect() as db:
            for start in range(0, len(ids), 500):
                batch = ids[start : start + 500]
                db.execute(
                    f"UPDATE inbox SET status = 'acked', acked_at = ?, result = ? "
                    f"WHERE id IN ({','.join('?' for _ in batch)})",
                    (datetime.now(timezone.utc).isoformat(), result, *batch),
                )

    def summary(self, datacenter_id: int) -> dict:
        """Counts are for retained frames; acknowledged frames expire after cleanup."""
        with self._connect() as db:
            counts = dict(db.execute("""
                SELECT CASE WHEN status = 'acked' THEN COALESCE(result, 'legacy')
                            ELSE status END, COUNT(*)
                FROM inbox WHERE datacenter_id = ? GROUP BY 1
            """, (datacenter_id,)))
            latest, oldest_unacked = db.execute("""
                SELECT MAX(received_at), MIN(CASE WHEN status != 'acked' THEN received_at END)
                FROM inbox WHERE datacenter_id = ?
            """, (datacenter_id,)).fetchone()
        return {"counts": counts, "latest_received_at": latest,
                "oldest_unacked_at": oldest_unacked}

    def cleanup(self, before: datetime) -> int:
        with self._connect() as db:
            result = db.execute(
                """DELETE FROM inbox WHERE id IN (
                    SELECT id FROM inbox WHERE status = 'acked' AND acked_at < ?
                    ORDER BY acked_at LIMIT 50000
                )""",
                (before.isoformat(),),
            )
        return result.rowcount
