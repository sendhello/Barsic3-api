import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.exceptions import HTTPException
from starlette import status

from constants import gen_db_name_enum, gen_report_name_enum
from services.settings import SettingsService, get_settings_service


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/new_services", response_model=list[str])
async def get_new_services(
    settings_service: Annotated[SettingsService, Depends(get_settings_service)],
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    report_name: Annotated[
        gen_report_name_enum(), Query(description="Наименование отчета")
    ] = None,
    other_report_name: Annotated[
        str | None, Query(description="Наименование отчета (если нет в списке)")
    ] = None,
) -> list[str]:
    """Поиск новых тарифов."""

    if report_name is not None:
        report_name_title = report_name.value
    elif other_report_name is not None:
        report_name_title = other_report_name
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Need any value from other_report_name or report_name",
        )

    new_tariffs = await settings_service.get_new_tariff(
        db_name.value, report_name_title
    )
    return new_tariffs
