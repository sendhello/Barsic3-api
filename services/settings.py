import logging

from services.bars import BarsService, get_bars_service
from services.report_config import ReportConfigService, get_report_config_service


logger = logging.getLogger(__name__)


class SettingsService:
    def __init__(
        self,
        bars_service: BarsService,
        report_config_service: ReportConfigService,
    ):
        self._bars_service = bars_service
        self._report_config_service = report_config_service

    def choose_db(self, db_name: str):
        self._bars_service.choose_db(db_name)

    async def get_new_tariff(self, report_name: str) -> list[str]:
        """Возвращает все нераспределенные тарифы."""

        all_tariffs = []
        organizations = self._bars_service.get_organisations()

        for organization in organizations:
            organization_tariffs = self._bars_service.get_tariffs(
                organization.super_account_id
            )
            all_tariffs.extend([tariff.name for tariff in organization_tariffs])

        distributed_tariffs = await self._report_config_service.get_report_elements(
            report_name
        )
        new_tariffs = list(
            set(all_tariffs) - set([tariff.title for tariff in distributed_tariffs])
        )

        return new_tariffs


def get_settings_service():
    return SettingsService(
        bars_service=get_bars_service(),
        report_config_service=get_report_config_service(),
    )
