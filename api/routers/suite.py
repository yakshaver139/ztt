"""Suite endpoints: read and update suite info."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.deps import get_db
from api.schemas import SuiteInfo, SuiteUpdate
from api.services.suite_service import SuiteService

router = APIRouter(prefix="/api", tags=["suites"])


@router.get("/suites/{suite_id}", response_model=SuiteInfo)
def get_suite(suite_id: int, db: Session = Depends(get_db)) -> SuiteInfo:
    suite = SuiteService(db).get(suite_id)
    if suite is None:
        raise HTTPException(status_code=404, detail="Suite not found")
    return suite


@router.patch("/suites/{suite_id}", response_model=SuiteInfo)
def update_suite(
    suite_id: int, payload: SuiteUpdate, db: Session = Depends(get_db)
) -> SuiteInfo:
    suite = SuiteService(db).update(suite_id, **payload.model_dump())
    if suite is None:
        raise HTTPException(status_code=404, detail="Suite not found")
    return suite
