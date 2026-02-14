import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette import status

from api.utils import PaginateQueryParams
from models.report import ReportElementModel
from schemas.report import (
    ReportElement,
    ReportElementCreate,
    ReportElementDetail,
    ReportElementUpdate,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def get_report_elements(
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)],
    report_group_id: Annotated[UUID, Query(description="ID группы отчета")] = None,
) -> list[ReportElement]:
    if report_group_id is not None:
        return await ReportElementModel.get_by_group_id(report_group_id=report_group_id)

    return await ReportElementModel.get_part(page=paginate.page, page_size=paginate.page_size)


@router.post("/")
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


@router.get("/{id}")
async def get_report_element(id: UUID) -> ReportElementDetail:
    report_element = await ReportElementModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ReportElement doesn't exists")

    return report_element


@router.put("/{id}")
async def update_report_element(id: UUID, report_element_in: ReportElementUpdate) -> ReportElement:
    report_element = await ReportElementModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ReportElement doesn't exists")

    report_element_dto = jsonable_encoder(report_element_in)
    return await report_element.update(**report_element_dto)


@router.delete("/{id}")
async def delete_report_element(id: UUID) -> ReportElement:
    report_element = await ReportElementModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ReportElement doesn't exists")

    return await report_element.delete()
