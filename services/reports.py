import logging
from datetime import date

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from starlette import status

from models.report_cache import ReportCacheModel
from schemas.report_cache import ReportCache, ReportCacheCreate


logger = logging.getLogger(__name__)


class ReportService:
    def __init__(self):
        pass

    async def get_report_by_date(
        self, report_type: str, report_date: date
    ) -> ReportCache | None:
        """Возвращает отчет по типу и дате."""

        report_cache_ = await ReportCacheModel.get_by_date(
            report_type=report_type, report_date=report_date
        )
        if not report_cache_:
            return None

        return ReportCache.model_validate(report_cache_)

    async def save_report(self, report_cache: ReportCacheCreate) -> None:
        report_cache_dto = jsonable_encoder(report_cache)
        try:
            await ReportCacheModel.create(**report_cache_dto)

        except IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="ReportCache with such 'date' and 'report_type' already exists",
            )

    async def delete_report(self, report_type: str, report_date: date) -> None:
        report_cache_ = await ReportCacheModel.get_by_date(
            report_type=report_type, report_date=report_date
        )
        if report_cache_:
            await report_cache_.delete()


def get_report_service():
    return ReportService()
