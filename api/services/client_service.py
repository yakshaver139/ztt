"""Client-facing business logic: contracts, racks, and utilization against
contracted capacity."""
from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from api.repositories.power_repository import PowerRepository
from api.repositories.rack_repository import RackRepository
from api.schemas import ClientUtilization
from db.models import Client, ClientContract, Rack


class ClientService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._racks = RackRepository(session)
        self._power = PowerRepository(session)

    def contracted_capacity_kw(self, client_id: int) -> float:
        """Total power the client is contracted for across all their suites."""
        total = (
            self.session.query(func.coalesce(func.sum(ClientContract.contracted_power_kw), 0.0))
            .filter(ClientContract.client_id == client_id)
            .scalar()
        )
        return float(total or 0.0)

    def racks(self, client_id: int) -> list[Rack]:
        return self._racks.for_client(client_id)

    def utilization(self, client_id: int) -> ClientUtilization | None:
        """Current draw across the client's racks as a percentage of their
        contracted capacity."""
        client = self.session.get(Client, client_id)
        if client is None:
            return None

        racks = self._racks.for_client(client_id)
        load_kw = 0.0
        for rack in racks:
            pdu = rack.rack_pdu
            if pdu is None:
                continue
            latest = self._power.latest_reading(pdu.id)
            if latest is not None:
                load_kw += latest.watts / 1000.0

        contracted = self.contracted_capacity_kw(client_id)
        utilization = (load_kw / contracted * 100.0) if contracted else 0.0
        return ClientUtilization(
            client_id=client.id,
            client_name=client.name,
            contracted_kw=round(contracted, 3),
            current_load_kw=round(load_kw, 3),
            utilization_pct=round(utilization, 2),
            rack_count=len(racks),
        )
