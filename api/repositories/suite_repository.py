"""Query helpers for suites."""
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from db.models import Suite


class SuiteRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, suite_id: int) -> Suite | None:
        return self.session.get(Suite, suite_id)

    def for_datacenter(self, datacenter_id: int) -> list[Suite]:
        return (
            self.session.query(Suite)
            .filter(Suite.datacenter_id == datacenter_id)
            .order_by(Suite.id)
            .all()
        )

    def update(self, suite_id: int, **fields: Any) -> Suite | None:
        """Apply the given non-null fields to a suite and persist. Returns the
        updated suite, or None if it doesn't exist."""
        suite = self.session.get(Suite, suite_id)
        if suite is None:
            return None
        for key, value in fields.items():
            setattr(suite, key, value)
        self.session.commit()
        self.session.refresh(suite)
        return suite
