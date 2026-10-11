"""Rack read-model: assemble a rack's summary (client, capacity, current load)."""
from __future__ import annotations

from sqlalchemy.orm import Session

from api.repositories.power_repository import PowerRepository
from api.repositories.rack_repository import RackRepository
from api.schemas import RackSummary
from db.models import Rack


class RackService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._racks = RackRepository(session)
        self._power = PowerRepository(session)

    def summary(self, rack_id: int) -> RackSummary | None:
        rack = self._racks.get(rack_id)
        return self._summary(rack) if rack is not None else None

    def summaries_for_suite(self, suite_id: int) -> list[RackSummary]:
        return [self._summary(rack) for rack in self._racks.for_suite(suite_id)]

    def summaries_for_client(self, client_id: int) -> list[RackSummary]:
        return [self._summary(rack) for rack in self._racks.for_client(client_id)]

    def _summary(self, rack: Rack) -> RackSummary:
        pdu = rack.rack_pdu
        capacity = pdu.capacity_limit_kw if pdu else None
        latest = self._power.latest_reading(pdu.id) if pdu else None
        load_kw = latest.watts / 1000.0 if latest is not None else None
        utilization = (
            load_kw / capacity * 100.0
            if load_kw is not None and capacity
            else None
        )
        return RackSummary(
            rack_id=rack.id,
            name=rack.name,
            client_id=rack.client_id,
            client_name=rack.client.name if rack.client else None,
            device_id=pdu.id if pdu else None,
            capacity_limit_kw=capacity,
            latest_load_kw=round(load_kw, 3) if load_kw is not None else None,
            utilization_pct=round(utilization, 2) if utilization is not None else None,
        )
