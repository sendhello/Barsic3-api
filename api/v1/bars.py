import logging
from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from constants import gen_db_name_enum
from schemas.bars import Category, ExtendedService, Organisation, TotalReport
from services.bars import BarsService, get_bars_service


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/tariffs", response_model=list[Category])
async def get_tariffs(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    organization_id: Annotated[int, Query(description="ID организации")],
    bars_service: BarsService = Depends(get_bars_service),
) -> list[Category]:
    """Список тарифов."""

    bars_service.choose_db(db_name=db_name.value)
    tariffs = bars_service.get_tariffs(organization_id=organization_id)
    return tariffs


@router.post("/organisations", response_model=list[Organisation])
async def get_organisations(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    bars_service: BarsService = Depends(get_bars_service),
) -> list[Organisation]:
    """Список Организаций."""

    bars_service.choose_db(db_name=db_name.value)
    organisations = bars_service.get_organisations()
    return organisations


@router.post("/total_report", response_model=TotalReport)
async def get_total_report(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    organization_id: int,
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time()),
    hide_zeroes: bool = False,
    hide_internal: bool = True,
    hide_discount: bool = False,
    bars_service: BarsService = Depends(get_bars_service),
) -> TotalReport:
    """Список Организаций."""

    bars_service.choose_db(db_name=db_name.value)
    return bars_service.get_total_report(
        organization_id=organization_id,
        date_from=date_from,
        date_to=date_to,
        hide_zeroes=hide_zeroes,
        hide_internal=hide_internal,
        hide_discount=hide_discount,
    )


@router.post(
    "/transactions_by_service_name_pattern", response_model=list[ExtendedService]
)
async def get_transactions_by_service_name_pattern(
    db_name: Annotated[gen_db_name_enum(), Query(description="База данных")],
    date_from: datetime = datetime.combine(date.today(), datetime.min.time()),
    date_to: datetime = datetime.combine(date.today(), datetime.min.time()),
    service_names: list[str] = Annotated[
        list[str], Query(description="Паттерн услуги для поиска клиентов")
    ],
    use_like: bool = True,
    bars_service: BarsService = Depends(get_bars_service),
) -> list[ExtendedService]:
    """Список купленных услуг группой клиентов."""

    bars_service.choose_db(db_name=db_name.value)
    return bars_service.get_transactions_by_service_name_pattern(
        date_from=date_from,
        date_to=date_to,
        service_names=service_names,
        use_like=use_like,
    )
