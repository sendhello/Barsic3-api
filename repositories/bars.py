from datetime import datetime

from pyodbc import ProgrammingError, Row

from core.settings import settings
from db.mssql import MsSqlDatabase
from repositories.base import BaseRepository
from sql.category import GET_TARIFFS_SQL
from sql.sp_report_totals_v2 import (
    SP_REPORT_TOTALS_V2_OLD_VERSION_SQL,
    SP_REPORT_TOTALS_V2_SQL,
)
from sql.super_account import GET_ORGANISATIONS_SQL


class BarsRepository(BaseRepository):
    def get_tariffs(self, organization_id: int) -> list[Row]:
        sql = GET_TARIFFS_SQL.format(organization_id=organization_id)
        return self._run_sql(sql)

    def get_organisations(self) -> list[Row]:
        sql = GET_ORGANISATIONS_SQL
        return self._run_sql(sql)

    def get_total_report(
        self,
        org: int,
        date_from: datetime,
        date_to: datetime,
        hide_zeroes: bool,
        hide_internal: bool,
        hide_discount: bool,
    ) -> list[Row]:
        """Формирование Итогового отчета."""

        date_from = date_from.isoformat()
        date_to = date_to.isoformat()
        hide_zeroes = int(hide_zeroes)
        hide_internal = int(hide_internal)
        hide_discount = int(hide_discount)

        sql = SP_REPORT_TOTALS_V2_SQL.format(
            org=org,
            date_from=date_from,
            date_to=date_to,
            hide_zeroes=hide_zeroes,
            hide_internal=hide_internal,
            hide_discount=hide_discount,
        )
        try:
            return self._run_sql(sql)

        except ProgrammingError:
            # Обратная совместимость для старой версии БД SkiBars
            sql = SP_REPORT_TOTALS_V2_OLD_VERSION_SQL.format(
                org=org,
                date_from=date_from,
                date_to=date_to,
                hide_zeroes=hide_zeroes,
                hide_internal=hide_internal,
            )
            return self._run_sql(sql)


def get_bars_repo() -> BarsRepository:
    db = MsSqlDatabase(
        server=settings.mssql_server,
        user=settings.mssql_user,
        password=settings.mssql_pwd,
    )
    return BarsRepository(db)
