"""Current finalized power snapshot and retained ingestion diagnostics."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from api.deps import get_db
from db.models import Datacenter, FinalizedPowerMinute, PowerDevice, PowerReading, Rack, Space, Suite
from ingestion.inbox import SQLiteInbox

router = APIRouter()


@router.get("/dashboard", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(Path(__file__).resolve().parents[1] / "dashboard.html")


@router.get("/api/datacenters/{datacenter_id}/snapshot")
def snapshot(datacenter_id: int, db: Session = Depends(get_db)) -> dict:
    dc = db.get(Datacenter, datacenter_id)
    if dc is None:
        raise HTTPException(status_code=404, detail="Datacenter not found")

    minute = db.scalar(select(func.max(FinalizedPowerMinute.timestamp)).where(
        FinalizedPowerMinute.datacenter_id == datacenter_id
    ))
    rows = db.execute(
        select(Suite.name, Space.row, Space.position, Rack.id, Rack.name,
               PowerDevice.id, PowerDevice.capacity_limit_kw, PowerReading.watts)
        .join(Rack, Rack.suite_id == Suite.id)
        .outerjoin(Space, Space.id == Rack.space_id)
        .outerjoin(PowerDevice, PowerDevice.id == Space.power_device_id)
        .outerjoin(PowerReading, and_(PowerReading.device_id == PowerDevice.id,
                                      PowerReading.timestamp == minute))
        .where(Suite.datacenter_id == datacenter_id)
        .order_by(Suite.id, Space.row, Space.position, Rack.id)
    ).all()
    racks = [{
        "suite": suite, "row": row, "position": position,
        "rack_id": rack_id, "name": name, "device_id": device_id,
        "capacity_kw": capacity, "load_kw": watts / 1000 if watts is not None else None,
    } for suite, row, position, rack_id, name, device_id, capacity, watts in rows]
    return {
        "datacenter": dc.name, "generated_at": datetime.now(timezone.utc).isoformat(),
        "minute": minute.isoformat() + "Z" if minute else None,
        "racks": racks, "inbox": SQLiteInbox().summary(datacenter_id),
    }
