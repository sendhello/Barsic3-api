import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from fastapi.exceptions import HTTPException
from starlette import status

from constants import gen_db_name_enum, gen_report_name_enum
from models.report import ReportNameModel, ReportGroupModel, ReportElementModel
from schemas.report import (
    ReportNameFullDetail, ReportNameDetail, ReportGroupDetail, ReportElement, ReportElementDetail
)
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

    settings_service.choose_db(db_name=db_name.value)
    new_tariffs = await settings_service.get_new_tariff(report_name_title)
    return new_tariffs


@router.get("/distributed_services", response_model=ReportNameFullDetail)
async def get_report_names(
    report_name: Annotated[
        gen_report_name_enum(), Query(description="Наименование отчета")
    ] = None,
) -> ReportNameFullDetail:

    if report_name is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Need any value from report_name")

    raw_report_name = await ReportNameModel.get_by_title(title=report_name.value)
    report_name_detail = ReportNameDetail.model_validate(raw_report_name)

    groups = []
    for group in report_name_detail.groups:
        raw_report_groups = await ReportElementModel.get_by_group_id(report_group_id=group.id)
        groups.append(
            ReportGroupDetail(
                id=group.id,
                title=group.title,
                parent_id=group.parent_id,
                report_name_id=group.report_name_id,
                elements=[
                    ReportElementDetail.model_validate(report_group) for report_group in raw_report_groups
                ]
            )
        )

    report_name_full_detail = ReportNameFullDetail(
        id=report_name_detail.id,
        title=report_name_detail.title,
        groups=groups
    )

    return report_name_full_detail


@router.get("/search_duplicates", response_model=dict)
async def get_report_names(
    report_name: Annotated[
        gen_report_name_enum(), Query(description="Наименование отчета")
    ] = None,
) -> dict:

    if report_name is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Need any value from report_name")

    raw_report_name = await ReportNameModel.get_by_title(title=report_name.value)
    report_name_detail = ReportNameDetail.model_validate(raw_report_name)

    elements = {}
    for group in report_name_detail.groups:
        raw_report_elements = await ReportElementModel.get_by_group_id(report_group_id=group.id)
        for raw_element in raw_report_elements:
            element = ReportElementDetail.model_validate(raw_element)
            element_group = elements.setdefault(element.title, [])
            element_group.append(group.title)

    res = {}
    for element_name, groups in elements.items():
        if len(groups) > 1:
            res[element_name] = groups

    return res
