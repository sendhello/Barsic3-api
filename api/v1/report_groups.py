import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette import status

from api.utils import PaginateQueryParams
from constants import gen_report_name_enum
from models.report import ReportGroupModel, ReportNameModel
from schemas.report import (
    ReportGroup,
    ReportGroupCreate,
    ReportGroupDetail,
    ReportGroupUpdate,
    ReportName,
)


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=list[ReportGroup])
async def get_report_groups(
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)]
) -> list[ReportGroup]:
    report_groups = await ReportGroupModel.get_part(
        page=paginate.page, page_size=paginate.page_size
    )
    return report_groups


@router.post("/", response_model=ReportGroup)
async def create_report_group(report_group_in: ReportGroupCreate) -> ReportGroup:
    report_group_dto = jsonable_encoder(report_group_in)
    try:
        report_group = await ReportGroupModel.create(**report_group_dto)

    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="ReportGroup with such title already exists",
        )

    return report_group


@router.get("/{id}", response_model=ReportGroupDetail)
async def get_report_group(id: UUID) -> ReportGroupDetail:
    report_group = await ReportGroupModel.get_by_id(id_=id)
    if not report_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportGroup doesn't exists"
        )

    return report_group


@router.put("/{id}", response_model=ReportGroup)
async def update_report_group(
    id: UUID, report_group_in: ReportGroupUpdate
) -> ReportGroup:
    report_group = await ReportGroupModel.get_by_id(id_=id)
    if not report_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportGroup doesn't exists"
        )

    report_group_dto = jsonable_encoder(report_group_in)
    report_group = await report_group.update(**report_group_dto)
    return report_group


@router.delete("/{id}", response_model=ReportGroup)
async def delete_report_group(id: UUID) -> ReportGroup:
    report_group = await ReportGroupModel.get_by_id(id_=id)
    if not report_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportGroup doesn't exists"
        )

    return await report_group.delete()


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
    """Добавление групп списком."""

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
