"""Suite read/update logic."""
from __future__ import annotations

from sqlalchemy.orm import Session

from api.repositories.suite_repository import SuiteRepository
from api.schemas import SuiteInfo


class SuiteService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._suites = SuiteRepository(session)

    def get(self, suite_id: int) -> SuiteInfo | None:
        suite = self._suites.get(suite_id)
        return SuiteInfo.model_validate(suite) if suite is not None else None

    def update(self, suite_id: int, **fields: object) -> SuiteInfo | None:
        changes = {key: value for key, value in fields.items() if value is not None}
        suite = self._suites.update(suite_id, **changes) if changes else self._suites.get(suite_id)
        return SuiteInfo.model_validate(suite) if suite is not None else None
