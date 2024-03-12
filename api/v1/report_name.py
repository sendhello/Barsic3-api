import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette import status

from api.utils import PaginateQueryParams
from models.report import ReportNameModel
from schemas.report import (
    ReportName,
    ReportNameCreate,
    ReportNameDetail,
    ReportNameUpdate,
)


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=list[ReportName])
async def get_report_names(
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)]
) -> list[ReportName]:
    report_names = await ReportNameModel.get_part(
        page=paginate.page, page_size=paginate.page_size
    )
    return report_names


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
