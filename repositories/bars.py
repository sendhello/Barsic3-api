from core.settings import settings
from db.mssql import MsSqlDatabase
from sql.category import get_tariffs
from sql.super_account import get_organisations


class BarsRepository:
    def __init__(self, db: MsSqlDatabase):
        self._db = db

    def _run_sql(self, sql: str):
        with self._db as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows

    def get_tariffs(self, db_name: str, organization_id: int):
        sql = get_tariffs(db_name, organization_id)
        return self._run_sql(sql)

    def get_organisations(self, db_name: str):
        sql = get_organisations(db_name)
        return self._run_sql(sql)


def get_bars_repo() -> BarsRepository:
    db = MsSqlDatabase(
        server=settings.mssql_server,
        database=settings.mssql_database1,
        user=settings.mssql_user,
        password=settings.mssql_pwd,
    )
    return BarsRepository(db)
