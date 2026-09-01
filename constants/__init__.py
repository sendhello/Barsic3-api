from .enums import MssqlDriverType, gen_db_name_enum, gen_report_name_enum

ANONYMOUS = "anonymous"
GOOGLE_DOC_VERSION = 15
GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

FREE_TARIFFS = ("Дети до 5 лет",)

# Основная организация аквапарка, по которой формируются отчеты.
# ООО «АКВАЛЭНД» — с 14.08.2026 сменило архивное ООО «ПАРК СЕРВИС» (36).
MAIN_COMPANY_ID = 7203673

# Категория зоны «Аквазона» (Category.CategoryId, StockType = 43)
AQUAZONE_CATEGORY_ID = 488

# Услуги, проходы по которым не считаются входом в Аквазону.
# Сопоставление по подстроке в Category.Name / ServicePoint.Name.
NOT_COUNTED_SERVICE_NAMES = ("Душ впечатлений",)

__all__ = (
    "ANONYMOUS",
    "AQUAZONE_CATEGORY_ID",
    "FREE_TARIFFS",
    "GOOGLE_DOC_VERSION",
    "GOOGLE_SCOPES",
    "MAIN_COMPANY_ID",
    "NOT_COUNTED_SERVICE_NAMES",
    "MssqlDriverType",
    "gen_db_name_enum",
    "gen_report_name_enum",
)
