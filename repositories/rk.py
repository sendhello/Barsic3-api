from datetime import datetime

from core.settings import settings
from db.mssql import MsSqlDatabase
from repositories.base import BaseRepository
from sql.rk_smile_request import RK_SMILE_TOTAL_SUM


class RKRepository(BaseRepository):
    def get_smile_report(
        self,
        date_from: datetime,
        date_to: datetime,
    ) -> dict:
        sql = RK_SMILE_TOTAL_SUM.format(
            date_from=date_from.strftime("%Y%m%d 00:00:00"),
            date_to=date_to.strftime("%Y%m%d 00:00:00"),
        )
        res = self._run_sql_to_dict(sql)
        return res[0]


def get_rk_repo() -> RKRepository:
    db = MsSqlDatabase(
        server=settings.mssql_server_rk,
        user=settings.mssql_user_rk,
        password=settings.mssql_pwd_rk,
    )
    db.set_database(settings.mssql_database_rk)
    return RKRepository(db)
