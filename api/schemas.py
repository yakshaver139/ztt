"""Pydantic request/response models for the API."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class DeviceUtilization(BaseModel):
    """Capacity/utilization summary for a single power device over the full
    reading history. For PDUs and the MSB the load is the roll-up of every
    metered rack PDU beneath the device; for a rack PDU it is its own readings.
    """
    device_id: int
    name: str
    device_type: str
    capacity_limit_kw: float
    peak_kw: float
    mean_kw: float
    utilization_pct: float   # peak_kw / capacity_limit_kw * 100
    hours_over_80pct: int    # count of samples whose load exceeds 80% of capacity


class RackSummary(BaseModel):
    """A rack plus its current power draw and utilization against its rack PDU."""
    rack_id: int
    name: str | None
    client_id: int | None
    client_name: str | None
    device_id: int | None            # the rack's metering rack PDU
    capacity_limit_kw: float | None
    latest_load_kw: float | None     # most recent per-minute average, kW
    utilization_pct: float | None    # latest_load_kw / capacity_limit_kw * 100


class ClientUtilization(BaseModel):
    """A client's current draw across all its racks vs its contracted capacity."""
    client_id: int
    client_name: str
    contracted_kw: float
    current_load_kw: float
    utilization_pct: float           # current_load_kw / contracted_kw * 100
    rack_count: int


class SuiteInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    datacenter_id: int
    name: str
    sla_capacity_kw: float | None


class SuiteUpdate(BaseModel):
    """Partial update for a suite; unset fields are left unchanged."""
    name: str | None = None
    sla_capacity_kw: float | None = None
