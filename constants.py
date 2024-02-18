from enum import Enum


ANONYMOUS = "anonymous"

GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


class MssqlDriverType(str, Enum):
    MICROSOFT_ODBC_18 = '{ODBC Driver 18 for SQL Server}'  # Microsoft ODBC driver for SQL Server (Linux)
    SQL_SERVER = '{SQL Server}'  # Windows Driver
