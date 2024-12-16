from abc import ABC

from pyodbc import Row

from db.mssql import MsSqlDatabase


class BaseRepository(ABC):
    def __init__(self, db: MsSqlDatabase):
        self._db = db

    def set_database(self, db_name: str) -> None:
        self._db.set_database(db_name)

    def _run_sql(self, sql: str) -> list[Row]:
        with self._db as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows

    def _run_sql_to_dict(self, sql: str) -> list[dict]:
        with self._db as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return results
