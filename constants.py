from enum import Enum, EnumType

from core.settings import settings


ANONYMOUS = "anonymous"

GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


class MssqlDriverType(str, Enum):
    MICROSOFT_ODBC_18 = "{ODBC Driver 18 for SQL Server}"  # Microsoft ODBC driver for SQL Server (Linux)
    SQL_SERVER = "{SQL Server}"  # Windows Driver


def gen_db_name_enum() -> EnumType:
    """Enum со списком баз данных."""

    values = [settings.mssql_database1, settings.mssql_database2]
    enum_members = {value: value for value in values}
    return Enum("DbName", enum_members)


def gen_report_name_enum() -> EnumType:
    """Enum со списком названий отчетов."""

    values = settings.report_names.split(",")
    enum_members = {value: value for value in values}
    return Enum("ReportName", enum_members)
