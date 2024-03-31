import logging
from datetime import datetime

from repositories.bars import BarsRepository, get_bars_repo
from schemas.bars import Category, Organisation, TotalReport, TotalReportElement


logger = logging.getLogger(__name__)


class BarsService:
    def __init__(self, repository: BarsRepository):
        self._repo = repository

    def choose_db(self, db_name: str):
        self._repo.set_database(db_name)

    def get_tariffs(self, organization_id: int) -> list[Category]:
        tariffs_ = self._repo.get_tariffs(organization_id)
        return [Category.model_validate(tariff) for tariff in tariffs_]

    def get_organisations(self) -> list[Organisation]:
        organisations_ = self._repo.get_organisations()
        return [Organisation.model_validate(org) for org in organisations_]

    def get_total_report(
        self,
        organization_id: int,
        date_from: datetime,
        date_to: datetime,
        hide_zeroes: bool,
        hide_internal: bool,
        hide_discount: bool,
    ) -> TotalReport:
        total_report_ = self._repo.get_total_report(
            org=organization_id,
            date_from=date_from,
            date_to=date_to,
            hide_zeroes=hide_zeroes,
            hide_internal=hide_internal,
            hide_discount=hide_discount,
        )
        return TotalReport(
            elements=[TotalReportElement.model_validate(el) for el in total_report_],
        )


def get_bars_service():
    return BarsService(repository=get_bars_repo())
