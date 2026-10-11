"""Capacity roll-up / utilization endpoints. Routers stay thin: parse, delegate,
return."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.deps import get_db
from api.schemas import DeviceUtilization
from api.services.rollup_service import RollupService

router = APIRouter(prefix="/api", tags=["rollup"])


@router.get(
    "/datacenters/{datacenter_id}/utilization",
    response_model=list[DeviceUtilization],
)
def datacenter_utilization(
    datacenter_id: int, db: Session = Depends(get_db)
) -> list[DeviceUtilization]:
    """Per-device peak/mean load and utilization for a datacenter, most utilized
    first. Feeds the capacity dashboard."""
    return RollupService(db).datacenter_utilization(datacenter_id)
