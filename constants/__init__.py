from .enums import MssqlDriverType, gen_db_name_enum, gen_report_name_enum

ANONYMOUS = "anonymous"
GOOGLE_DOC_VERSION = 14
GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

FREE_TARIFFS = ("Дети до 5 лет",)

MAIN_COMPANY_ID = 7203673

__all__ = (
    "ANONYMOUS",
    "FREE_TARIFFS",
    "GOOGLE_DOC_VERSION",
    "GOOGLE_SCOPES",
    "MssqlDriverType",
    "gen_db_name_enum",
    "gen_report_name_enum",
)
