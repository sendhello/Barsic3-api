import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from constants import gen_db_name_enum
from schemas.bars import Category, Organisation
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

    tariffs = bars_service.get_tariffs(
        db_name=db_name.value, organization_id=organization_id
    )
    return tariffs


@router.post("/organisations", response_model=list[Organisation])
async def get_organisations(
    db_name: str,
    bars_service: BarsService = Depends(get_bars_service),
) -> list[Organisation]:
    """Список Организаций."""

    organisations = bars_service.get_organisations(db_name=db_name)
    return organisations
