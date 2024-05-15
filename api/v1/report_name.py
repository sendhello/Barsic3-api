import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette import status

from api.utils import PaginateQueryParams
from constants import gen_report_name_enum
from models.report import ReportGroupModel, ReportNameModel
from schemas.report import (
    ReportGroup,
    ReportName,
    ReportNameCreate,
    ReportNameDetail,
    ReportNameUpdate,
)


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=list[ReportName])
async def get_report_names(
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)],
    report_name: Annotated[
        gen_report_name_enum(), Query(description="Наименование отчета")
    ] = None,
) -> list[ReportName]:

    if report_name is not None:
        raw_report_name = await ReportNameModel.get_by_title(title=report_name.value)
        return [raw_report_name]

    raw_report_names = await ReportNameModel.get_part(
        page=paginate.page, page_size=paginate.page_size
    )
    return raw_report_names


@router.post("/", response_model=ReportName)
async def create_report_name(report_name_in: ReportNameCreate) -> ReportName:
    report_name_dto = jsonable_encoder(report_name_in)
    try:
        report_names = await ReportNameModel.create(**report_name_dto)

    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="ReportName with such title already exists",
        )

    return report_names


@router.get("/{id}", response_model=ReportNameDetail)
async def get_report_name(id: UUID) -> ReportNameDetail:
    report_name = await ReportNameModel.get_by_id(id_=id)
    if not report_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportName doesn't exists"
        )

    return report_name


@router.put("/{id}", response_model=ReportName)
async def update_report_name(id: UUID, report_name_in: ReportNameUpdate) -> ReportName:
    report_name = await ReportNameModel.get_by_id(id_=id)
    if not report_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportName doesn't exists"
        )

    report_name_dto = jsonable_encoder(report_name_in)
    report_name = await report_name.update(**report_name_dto)
    return report_name


@router.delete("/{id}", response_model=ReportName)
async def delete_report_name(id: UUID) -> ReportName:
    report_name = await ReportNameModel.get_by_id(id_=id)
    if not report_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportName doesn't exists"
        )

    return await report_name.delete()


@router.post("/{id}/add_groups/", response_model=list[ReportGroup])
async def add_elements(
    id: Annotated[UUID, Path(description="ID группы")],
    report_groups: Annotated[list[str], Body(description="Список групп отчета")],
) -> list[ReportGroup]:
    """Добавление групп в отчет."""

    created_groups = []
    error_groups = []
    for group_name in report_groups:
        try:
            db_report_group = await ReportGroupModel.create(
                title=group_name, parent_id=None, report_name_id=id
            )
            created_groups.append(ReportGroup.model_validate(db_report_group))

        except IntegrityError:
            error_groups.append(group_name)

    if error_groups:
        error_groups = [f'"{error}"' for error in error_groups]
        error_groups_text = f'{", ".join(error_groups)}'
        error_message = f"Groups {error_groups_text} already exist"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error_message
        )

    return created_groups


@router.post("/add_groups", response_model=list[ReportGroup])
async def create_report_groups(
    report_groups: Annotated[list[str], Body(description="Список групп отчета")],
    report_name: Annotated[
        gen_report_name_enum(), Query(description="Наименование отчета")
    ] = None,
    other_report_name: Annotated[
        str | None, Query(description="Наименование отчета (если нет в списке)")
    ] = None,
) -> list[ReportGroup]:
    """Добавление групп списком. (Для ручного добавления)"""

    if report_name is not None:
        report_name_title = report_name.value
    elif other_report_name is not None:
        report_name_title = other_report_name
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Need any value from other_report_name or report_name",
        )

    report_name_ = await ReportNameModel.get_by_title(report_name_title)
    if report_name_ is None:
        report_names = await ReportNameModel.get_all()
        report_names_text = ", ".join(f'"{name.title}"' for name in report_names)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Report with name '{report_name_title}' not found in database. "
            f"Please try one from: {report_names_text}",
        )

    report_name = ReportName.model_validate(report_name_)

    created_groups = []
    error_groups = []
    for group_name in report_groups:
        try:
            report_group_ = await ReportGroupModel.create(
                title=group_name, parent_id=None, report_name_id=report_name.id
            )
            created_groups.append(ReportGroup.model_validate(report_group_))

        except IntegrityError:
            error_groups.append(group_name)

    if error_groups:
        error_groups = [f'"{error}"' for error in error_groups]
        error_groups_text = f'{", ".join(error_groups)}'
        error_message = f"Groups {error_groups_text} already exist"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error_message
        )

    return created_groups
