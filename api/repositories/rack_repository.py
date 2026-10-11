"""Query helpers for racks."""
from __future__ import annotations

from sqlalchemy.orm import Session

from db.models import Rack


class RackRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, rack_id: int) -> Rack | None:
        return self.session.get(Rack, rack_id)

    def for_suite(self, suite_id: int) -> list[Rack]:
        return (
            self.session.query(Rack)
            .filter(Rack.suite_id == suite_id)
            .order_by(Rack.id)
            .all()
        )

    def for_client(self, client_id: int) -> list[Rack]:
        return (
            self.session.query(Rack)
            .filter(Rack.client_id == client_id)
            .order_by(Rack.id)
            .all()
        )
