from fastapi.routing import APIRouter

from .bars import router as bars_router
from .google_report_ids import router as google_report_router
from .report_elements import router as report_elements_router
from .report_groups import router as report_groups_router
from .report_name import router as report_name_router
from .report_settings import router as report_settings_router
from .reports import router as reports_router


router = APIRouter()
router.include_router(reports_router, prefix="/reports", tags=["Reports"])
router.include_router(bars_router, prefix="/bars", tags=["Bars"])
router.include_router(report_name_router, prefix="/report_name", tags=["Report Name"])
router.include_router(
    report_groups_router, prefix="/report_group", tags=["Report Group"]
)
router.include_router(
    report_elements_router, prefix="/report_element", tags=["Report Elements"]
)
router.include_router(
    google_report_router, prefix="/google_report_id", tags=["Google Report Id"]
)
router.include_router(
    report_settings_router, prefix="/report_settings", tags=["Report Settings"]
)
