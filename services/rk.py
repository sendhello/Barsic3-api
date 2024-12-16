import logging
from datetime import datetime

from repositories.rk import RKRepository, get_rk_repo
from schemas.rk import SmileReport


logger = logging.getLogger(__name__)


class RKService:
    def __init__(self, repository: RKRepository):
        self._repo = repository

    def choose_db(self, db_name: str):
        self._repo.set_database(db_name)

    def get_smile_report(
        self,
        date_from: datetime,
        date_to: datetime,
    ) -> SmileReport:
        raw_smile_report = self._repo.get_smile_report(
            date_from=date_from,
            date_to=date_to,
        )
        return SmileReport.model_validate(raw_smile_report)


def get_rk_service():
    return RKService(repository=get_rk_repo())
