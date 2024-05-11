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
from models.report import ReportElementModel, ReportGroupModel, ReportNameModel
from schemas.report import (
    ReportElement,
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
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)],
    report_name_id: Annotated[UUID, Query(description="ID наименования отчета")] = None,
) -> list[ReportGroup]:

    if report_name_id is not None:
        raw_report_groups = await ReportGroupModel.get_by_report_name_id(
            report_name_id=report_name_id
        )
        return raw_report_groups

    raw_report_groups = await ReportGroupModel.get_part(
        page=paginate.page, page_size=paginate.page_size
    )
    return raw_report_groups


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


@router.post("/{id}/add_elements/", response_model=list[ReportElement])
async def add_elements(
    id: Annotated[UUID, Path(description="ID группы")],
    elements: Annotated[
        list[str], Body(description="Список тарифов для добавления в группу")
    ],
) -> list[ReportElement]:
    """Добавление элементов в группу."""

    created_elements = []
    error_elements = []
    for el in elements:
        try:
            db_element = await ReportElementModel.create(
                title=el,
                group_id=id,
            )
            created_elements.append(ReportElement.model_validate(db_element))

        except IntegrityError:
            error_elements.append(el)

    if error_elements:
        error_elements = [f'"{error}"' for error in error_elements]
        error_elements_text = f'{", ".join(error_elements)}'
        error_message = f"Elements {error_elements_text} already exist"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error_message
        )

    return created_elements


@router.post("/add_elements", response_model=list[ReportElement])
async def create_report_elements(
    elements: Annotated[list[str], Body(description="Список тарифов в группе")],
    group_name: Annotated[str, Query(description="Группа отчета")],
    report_name: Annotated[
        gen_report_name_enum(), Query(description="Наименование отчета")
    ] = None,
    other_report_name: Annotated[
        str | None, Query(description="Наименование отчета (если нет в списке)")
    ] = None,
) -> list[ReportElement]:
    """Добавление тарифов в группу списком (для ручного добавления)."""

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

    group_ = await ReportGroupModel.get_by_title(group_name, report_name.id)
    if group_ is None:
        group_names = await ReportGroupModel.get_all()
        group_names_text = ", ".join(f'"{name.title}"' for name in group_names)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Report with name '{group_name}' not found in database. "
            f"Please try one from: {group_names_text}",
        )

    group = ReportGroup.model_validate(group_)
    created_elements = []
    error_elements = []
    for el in elements:
        try:
            element_ = await ReportElementModel.create(
                title=el,
                group_id=group.id,
            )
            created_elements.append(ReportElement.model_validate(element_))

        except IntegrityError:
            error_elements.append(el)

    if error_elements:
        error_elements = [f'"{error}"' for error in error_elements]
        error_elements_text = f'{", ".join(error_elements)}'
        error_message = f"Elements {error_elements_text} already exist"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=error_message
        )

    return created_elements
