import logging
from datetime import datetime
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError
from starlette import status

from models.report import (
    GoogleReportIdModel,
    ReportElementModel,
    ReportNameModel,
)
from schemas.google_report_ids import GoogleReportId, GoogleReportIdCreate
from schemas.report import ReportElement, ReportGroup, ReportName, ReportNameDetail


logger = logging.getLogger(__name__)


class ReportConfigService:
    def __init__(
        self,
    ):
        pass

    async def get_report_names(self) -> list[str]:
        report_names = [
            ReportName.model_validate(report_name_).title
            for report_name_ in await ReportNameModel.get_all()
        ]
        return report_names

    async def get_report_groups(self, report_name: str) -> list[ReportGroup]:
        report_ = await ReportNameModel.get_by_title(report_name)
        if report_ is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Report with name '{report_name}' not found.",
            )

        report = ReportNameDetail.model_validate(report_)
        return report.groups

    async def get_report_elements(self, report_name: str) -> list[ReportElement]:
        """Получение всех элементов отчета."""

        all_elements = []
        report_groups = await self.get_report_groups(report_name)

        for group in report_groups:
            elements_ = await ReportElementModel.get_by_group_id(
                report_group_id=group.id
            )
            elements = [ReportElement.model_validate(el) for el in elements_]
            all_elements.extend(elements)

        return all_elements

    async def get_report_elements_with_groups(
        self, report_name: str
    ) -> dict[str, list[str]]:
        """Получение всех элементов отчета по группам."""

        all_elements = {}
        report_groups = await self.get_report_groups(report_name)

        for group in report_groups:
            elements_ = await ReportElementModel.get_by_group_id(
                report_group_id=group.id
            )
            elements = [ReportElement.model_validate(el) for el in elements_]
            all_elements[group.title] = [el.title for el in elements]

        return all_elements

    async def get_google_doc_ids(self) -> list[GoogleReportId]:
        google_doc_ids_ = await GoogleReportIdModel.get_all()

        google_doc_ids = [
            GoogleReportId.model_validate(google_doc_id)
            for google_doc_id in google_doc_ids_
        ]
        return google_doc_ids

    async def get_google_doc_id_by_id(self, id_: UUID) -> GoogleReportId:
        google_doc_id_ = await GoogleReportIdModel.get_by_id(id_)

        google_doc_id = GoogleReportId.model_validate(google_doc_id_)
        return google_doc_id

    async def get_financial_doc_id_by_date(
        self, date_: datetime
    ) -> GoogleReportId | None:
        month = date_.strftime("%Y-%m")
        google_doc_id_ = await GoogleReportIdModel.get_by_month(
            month, report_type="financial"
        )
        if google_doc_id_ is None:
            return None

        google_doc_id = GoogleReportId.model_validate(google_doc_id_)
        return google_doc_id

    async def get_total_detail_doc_id_by_date(
        self, date_: datetime
    ) -> GoogleReportId | None:
        month = date_.strftime("%Y-%m")
        google_doc_id_ = await GoogleReportIdModel.get_by_month(
            month, report_type="total_detail"
        )
        if google_doc_id_ is None:
            return None

        google_doc_id = GoogleReportId.model_validate(google_doc_id_)
        return google_doc_id

    async def add_google_report_id(
        self, google_report_id: GoogleReportIdCreate
    ) -> None:
        google_doc_id_dto = jsonable_encoder(google_report_id)
        try:
            await GoogleReportIdModel.create(**google_doc_id_dto)

        except IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="GoogleReportId with such title already exists",
            )


def get_report_config_service():
    return ReportConfigService()
