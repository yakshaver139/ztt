"""Roll up metered rack-PDU load through the power tree and summarize each
device's utilization against its capacity.

Powers the capacity dashboard's "which devices are closest to their limit?"
view. Only rack PDUs are metered; a PDU's or the MSB's load is the sum of the
readings of every rack PDU beneath it.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from sqlalchemy.orm import Session

from api.repositories.power_repository import PowerRepository
from api.schemas import DeviceUtilization
from db.models import PowerDevice


class RollupService:
    def __init__(self, session: Session) -> None:
        self.repo = PowerRepository(session)

    def datacenter_utilization(self, datacenter_id: int) -> list[DeviceUtilization]:
        """Utilization summary for every device in the datacenter, ordered from
        most to least utilized."""
        devices = self.repo.devices_for_datacenter(datacenter_id)

        # Child map (source -> targets) for walking the tree down to leaves.
        self._children: dict[int, list[int]] = defaultdict(list)
        for conn in self.repo.connections_for_datacenter(datacenter_id):
            self._children[conn.source_id].append(conn.target_id)
        self._leaf_ids = {d.id for d in devices if d.device_type == "rack_pdu"}

        summaries = [self._summarize(device) for device in devices]
        summaries.sort(key=lambda u: u.utilization_pct, reverse=True)
        return summaries

    def _descendant_leaf_ids(self, device: PowerDevice) -> set[int]:
        if device.id in self._leaf_ids:
            return {device.id}
        leaves: set[int] = set()
        stack = list(self._children.get(device.id, []))
        while stack:
            node = stack.pop()
            if node in self._leaf_ids:
                leaves.add(node)
            else:
                stack.extend(self._children.get(node, []))
        return leaves

    def _rolled_up_series(self, device: PowerDevice) -> dict[datetime, float]:
        """Sum the load (watts) of every metered rack PDU beneath `device`,
        bucketed by timestamp."""
        member_ids = self._descendant_leaf_ids(device)
        series: dict[datetime, float] = defaultdict(float)
        for reading in self.repo.readings_in_datacenter(device.datacenter_id):
            if reading.device_id in member_ids:
                series[reading.timestamp] += reading.watts
        return series

    def _summarize(self, device: PowerDevice) -> DeviceUtilization:
        series = self._rolled_up_series(device)
        loads_kw = [w / 1000.0 for w in series.values()]

        peak_kw = max(loads_kw) if loads_kw else 0.0
        mean_kw = sum(loads_kw) / len(loads_kw) if loads_kw else 0.0
        cap = device.capacity_limit_kw
        utilization_pct = (peak_kw / cap * 100.0) if cap else 0.0
        hours_over = sum(1 for kw in loads_kw if cap and kw > 0.8 * cap)

        return DeviceUtilization(
            device_id=device.id,
            name=device.name,
            device_type=device.device_type,
            capacity_limit_kw=round(cap, 3),
            peak_kw=round(peak_kw, 3),
            mean_kw=round(mean_kw, 3),
            utilization_pct=round(utilization_pct, 2),
            hours_over_80pct=hours_over,
        )
