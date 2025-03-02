import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette import status

from api.utils import PaginateQueryParams
from models.report import GoogleReportIdModel
from schemas.google_report_ids import (
    GoogleReportId,
    GoogleReportIdCreate,
    GoogleReportIdUpdate,
)


router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=list[GoogleReportId])
async def get_report_elements(
    paginate: Annotated[PaginateQueryParams, Depends(PaginateQueryParams)],
) -> list[GoogleReportId]:
    report_elements = await GoogleReportIdModel.get_part(
        page=paginate.page, page_size=paginate.page_size
    )
    return report_elements


@router.post("/", response_model=GoogleReportId)
async def create_report_element(
    report_element_in: GoogleReportIdCreate,
) -> GoogleReportId:
    report_element_dto = jsonable_encoder(report_element_in)
    try:
        report_elements = await GoogleReportIdModel.create(**report_element_dto)

    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="GoogleReportId with such title already exists",
        )

    return report_elements


@router.get("/{id}", response_model=GoogleReportId)
async def get_report_element(id: UUID) -> GoogleReportId:
    report_element = await GoogleReportIdModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="GoogleReportId doesn't exists",
        )

    return report_element


@router.put("/{id}", response_model=GoogleReportId)
async def update_report_element(
    id: UUID, report_element_in: GoogleReportIdUpdate
) -> GoogleReportId:
    report_element = await GoogleReportIdModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="GoogleReportId doesn't exists",
        )

    report_element_dto = jsonable_encoder(report_element_in)
    report_element = await report_element.update(**report_element_dto)
    return report_element


@router.delete("/{id}", response_model=GoogleReportId)
async def delete_report_element(id: UUID) -> GoogleReportId:
    report_element = await GoogleReportIdModel.get_by_id(id_=id)
    if not report_element:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="GoogleReportId doesn't exists",
        )

    return await report_element.delete()
