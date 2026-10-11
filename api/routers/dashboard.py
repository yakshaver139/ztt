"""Current finalized power snapshot and retained ingestion diagnostics."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import and_, func, inspect, select
from sqlalchemy.orm import Session

from api.deps import get_db
from db.models import (Datacenter, FinalizedPowerMinute, PowerDevice, PowerMinuteCoverage,
                       PowerReading, Rack, Space, Suite)
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
    coverage = (db.get(PowerMinuteCoverage, (datacenter_id, minute))
                if minute and inspect(db.get_bind()).has_table("power_minute_coverage") else None)
    last_reading = (select(PowerReading.timestamp)
                    .where(PowerReading.device_id == PowerDevice.id)
                    .order_by(PowerReading.timestamp.desc()).limit(1)
                    .correlate(PowerDevice).scalar_subquery())
    rows = db.execute(
        select(Suite.name, Space.row, Space.position, Rack.id, Rack.name,
               PowerDevice.id, PowerDevice.capacity_limit_kw, PowerReading.watts,
               last_reading)
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
        "last_reading_at": last_seen.isoformat() + "Z" if last_seen else None,
    } for suite, row, position, rack_id, name, device_id, capacity, watts, last_seen in rows]
    return {
        "datacenter": dc.name, "generated_at": datetime.now(timezone.utc).isoformat(),
        "minute": minute.isoformat() + "Z" if minute else None,
        "racks": racks, "sample_counts": coverage.sample_counts if coverage else None,
        "inbox": SQLiteInbox().summary(datacenter_id),
    }
