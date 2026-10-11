"""Query helpers for the power hierarchy.

Repositories wrap SQLAlchemy queries per aggregate; services compose them. Keep
HTTP concerns out of here and query concerns out of services.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from db.models import PowerConnection, PowerDevice, PowerReading


class PowerRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get(self, device_id: int) -> PowerDevice | None:
        return self.session.get(PowerDevice, device_id)

    def children(self, device_id: int) -> list[PowerDevice]:
        """Direct downstream devices fed by ``device_id``."""
        return (
            self.session.query(PowerDevice)
            .join(PowerConnection, PowerConnection.target_id == PowerDevice.id)
            .filter(PowerConnection.source_id == device_id)
            .all()
        )

    def descendants(self, device_id: int) -> list[PowerDevice]:
        """Every device beneath ``device_id`` in the power tree (breadth-first)."""
        found: list[PowerDevice] = []
        seen: set[int] = set()
        frontier = [device_id]
        while frontier:
            rows = (
                self.session.query(PowerConnection.target_id)
                .filter(PowerConnection.source_id.in_(frontier))
                .all()
            )
            next_ids = [tid for (tid,) in rows if tid not in seen]
            if not next_ids:
                break
            seen.update(next_ids)
            found.extend(
                self.session.query(PowerDevice)
                .filter(PowerDevice.id.in_(next_ids))
                .all()
            )
            frontier = next_ids
        return found

    def latest_reading(self, device_id: int) -> PowerReading | None:
        return (
            self.session.query(PowerReading)
            .filter(PowerReading.device_id == device_id)
            .order_by(PowerReading.timestamp.desc())
            .first()
        )

    def devices_for_datacenter(self, datacenter_id: int) -> list[PowerDevice]:
        return (
            self.session.query(PowerDevice)
            .filter(PowerDevice.datacenter_id == datacenter_id)
            .all()
        )

    def connections_for_datacenter(self, datacenter_id: int) -> list[PowerConnection]:
        return (
            self.session.query(PowerConnection)
            .join(PowerDevice, PowerConnection.source_id == PowerDevice.id)
            .filter(PowerDevice.datacenter_id == datacenter_id)
            .all()
        )

    def readings_in_datacenter(self, datacenter_id: int) -> list[PowerReading]:
        """Every metered reading in the datacenter (only rack PDUs are metered)."""
        return (
            self.session.query(PowerReading)
            .join(PowerDevice, PowerReading.device_id == PowerDevice.id)
            .filter(PowerDevice.datacenter_id == datacenter_id)
            .all()
        )
