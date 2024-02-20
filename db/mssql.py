import pyodbc

from constants import MssqlDriverType
from core.settings import settings


class MsSqlConnection:
    def __init__(self, driver, server, database, uid, pwd):
        self.driver = driver
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd

    def connect(self):
        return pyodbc.connect(
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"PORT=1433;"
            f"DATABASE={self.database};"
            f"UID={self.uid};"
            f"PWD={self.pwd};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no"
        )


def get_mssql_connection(server, database, uid, pwd):
    mssql_connection = MsSqlConnection(
        driver=getattr(MssqlDriverType, settings.mssql_driver_type).value,
        server=server,
        database=database,
        uid=uid,
        pwd=pwd,
    )
    return mssql_connection.connect()
