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
from models.report import ReportElementModel, ReportGroupModel, ReportNameModel
from schemas.report import (
    ReportElement,
    ReportElementCreate,
    ReportElementDetail,
    ReportElementUpdate,
    ReportGroup,
    ReportName,
)


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=list[ReportElement])
async def get_report_elements(
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)]
) -> list[ReportElement]:
    report_elements = await ReportElementModel.get_part(
        page=paginate.page, page_size=paginate.page_size
    )
    return report_elements


@router.post("/", response_model=ReportElement)
async def create_report_element(
    report_element_in: ReportElementCreate,
) -> ReportElement:
    report_element_dto = jsonable_encoder(report_element_in)
    try:
        report_elements = await ReportElementModel.create(**report_element_dto)

    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="ReportElement with such title already exists",
        )

    return report_elements


@router.get("/{id}", response_model=ReportElementDetail)
async def get_report_element(id: UUID) -> ReportElementDetail:
    report_element = await ReportElementModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportElement doesn't exists"
        )

    return report_element


@router.put("/{id}", response_model=ReportElement)
async def update_report_element(
    id: UUID, report_element_in: ReportElementUpdate
) -> ReportElement:
    report_element = await ReportElementModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportElement doesn't exists"
        )

    report_element_dto = jsonable_encoder(report_element_in)
    report_element = await report_element.update(**report_element_dto)
    return report_element


@router.delete("/{id}", response_model=ReportElement)
async def delete_report_element(id: UUID) -> ReportElement:
    report_element = await ReportElementModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ReportElement doesn't exists"
        )

    return await report_element.delete()


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
    """Добавление тарифов в группу списком."""

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
