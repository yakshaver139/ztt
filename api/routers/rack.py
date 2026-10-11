"""Rack endpoints: individual and per-suite summaries."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.deps import get_db
from api.schemas import RackSummary
from api.services.rack_service import RackService

router = APIRouter(prefix="/api", tags=["racks"])


@router.get("/racks/{rack_id}", response_model=RackSummary)
def get_rack(rack_id: int, db: Session = Depends(get_db)) -> RackSummary:
    summary = RackService(db).summary(rack_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="Rack not found")
    return summary


@router.get("/suites/{suite_id}/racks", response_model=list[RackSummary])
def racks_for_suite(suite_id: int, db: Session = Depends(get_db)) -> list[RackSummary]:
    return RackService(db).summaries_for_suite(suite_id)
