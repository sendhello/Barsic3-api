import logging
from typing import Optional

from pyodbc import Connection, connect

from constants import MssqlDriverType
from core.settings import settings
from utils.backoff import backoff


logger = logging.getLogger(__name__)


class MsSqlDatabase:
    def __init__(self, server: str, user: str, password: str, port: int = 1433):
        self._server = server
        self._port = port
        self._database = None
        self._user = user
        self._password = password
        self._connection: Optional[Connection] = None

    @property
    def _driver(self) -> str:
        return getattr(MssqlDriverType, settings.mssql_driver_type).value

    def set_database(self, database: str) -> None:
        self._database = database

    @backoff()
    def _connect(self):
        self._connection = connect(
            f"DRIVER={self._driver};"
            f"SERVER={self._server};"
            f"PORT={self._port};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no"
        )

    def disconnect(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def connect(self) -> Connection:
        if self._connection is None:
            self._connect()

        return self._connection

    def __enter__(self) -> Connection:
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()


def mssql_connection(server, database, user, password) -> Connection:
    db = MsSqlDatabase(
        server=server,
        user=user,
        password=password,
    )
    db.set_database(database)
    return db.connect()
