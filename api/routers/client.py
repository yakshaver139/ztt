"""Client endpoints: contracted capacity, racks, and utilization."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.deps import get_db
from api.schemas import ClientUtilization, RackSummary
from api.services.client_service import ClientService
from api.services.rack_service import RackService

router = APIRouter(prefix="/api", tags=["clients"])


@router.get("/clients/{client_id}/utilization", response_model=ClientUtilization)
def client_utilization(
    client_id: int, db: Session = Depends(get_db)
) -> ClientUtilization:
    """The client's current draw as a percentage of their contracted capacity."""
    result = ClientService(db).utilization(client_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Client not found")
    return result


@router.get("/clients/{client_id}/racks", response_model=list[RackSummary])
def client_racks(client_id: int, db: Session = Depends(get_db)) -> list[RackSummary]:
    """Summaries of every rack rented by the client."""
    return RackService(db).summaries_for_client(client_id)
