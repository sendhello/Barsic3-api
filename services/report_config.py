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
        return [ReportName.model_validate(report_name_).title for report_name_ in await ReportNameModel.get_all()]

    async def get_report_groups(self, report_name: str, exclude: list[str] = None) -> list[ReportGroup]:
        report_ = await ReportNameModel.get_by_title(report_name)
        if report_ is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Report with name '{report_name}' not found.",
            )

        report = ReportNameDetail.model_validate(report_)
        if exclude:
            report.groups = [group for group in report.groups if group.title not in exclude]

        return report.groups

    async def get_report_elements(self, report_name: str) -> list[ReportElement]:
        """Получение всех элементов отчета."""

        all_elements = []
        report_groups = await self.get_report_groups(report_name)

        for group in report_groups:
            elements_ = await ReportElementModel.get_by_group_id(report_group_id=group.id)
            elements = [ReportElement.model_validate(el) for el in elements_]
            all_elements.extend(elements)

        return all_elements

    async def get_report_elements_with_groups(self, report_name: str) -> dict[str, list[str]]:
        """Получение всех элементов отчета по группам."""

        all_elements = {}
        report_groups = await self.get_report_groups(report_name)

        for group in report_groups:
            elements_ = await ReportElementModel.get_by_group_id(report_group_id=group.id)
            elements = [ReportElement.model_validate(el) for el in elements_]
            all_elements[group.title] = [el.title for el in elements]

        return all_elements

    async def get_report_tree(self, report_name: str) -> dict[str, dict | list[str]]:
        """Получение дерева групп отчета, где в листьях находятся названия элементов."""

        report_groups = await self.get_report_groups(report_name, exclude=["Не учитывать"])

        children_by_parent: dict[UUID | None, list[ReportGroup]] = {}
        elements_by_group: dict[UUID, list[str]] = {}

        for group in report_groups:
            children_by_parent.setdefault(group.parent_id, []).append(group)

            elements_ = await ReportElementModel.get_by_group_id(report_group_id=group.id)
            elements_by_group[group.id] = [ReportElement.model_validate(el).title for el in elements_]

        root_groups = [group for group in report_groups if group.parent_id is None]

        def build_group_tree(group: ReportGroup) -> dict | list[str]:
            nested_groups = children_by_parent.get(group.id, [])
            if not nested_groups:
                return elements_by_group.get(group.id, [])

            return {nested_group.title: build_group_tree(nested_group) for nested_group in nested_groups}

        return {group.title: build_group_tree(group) for group in root_groups}

    async def get_google_doc_ids(self) -> list[GoogleReportId]:
        google_doc_ids_ = await GoogleReportIdModel.get_all()

        return [GoogleReportId.model_validate(google_doc_id) for google_doc_id in google_doc_ids_]

    async def get_google_doc_id_by_id(self, id_: UUID) -> GoogleReportId:
        google_doc_id_ = await GoogleReportIdModel.get_by_id(id_)

        return GoogleReportId.model_validate(google_doc_id_)

    async def get_financial_doc_id_by_date(self, date_: datetime) -> GoogleReportId | None:
        month = date_.strftime("%Y-%m")
        google_doc_id_ = await GoogleReportIdModel.get_by_month(month, report_type="financial")
        if google_doc_id_ is None:
            return None

        return GoogleReportId.model_validate(google_doc_id_)

    async def get_total_detail_doc_id_by_date(self, date_: datetime) -> GoogleReportId | None:
        month = date_.strftime("%Y-%m")
        google_doc_id_ = await GoogleReportIdModel.get_by_month(month, report_type="total_detail")
        if google_doc_id_ is None:
            return None

        return GoogleReportId.model_validate(google_doc_id_)

    async def get_attendance_doc_id_by_date(self, date_: datetime) -> GoogleReportId | None:
        month = date_.strftime("%Y-%m")
        google_doc_id_ = await GoogleReportIdModel.get_by_month(month, report_type="attendance")
        if google_doc_id_ is None:
            return None

        return GoogleReportId.model_validate(google_doc_id_)

    async def add_google_report_id(self, google_report_id: GoogleReportIdCreate) -> None:
        google_doc_id_dto = jsonable_encoder(google_report_id)
        try:
            await GoogleReportIdModel.create(**google_doc_id_dto)

        except IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="GoogleReportId with such title already exists",
            )

    async def save_google_report_id(self, google_report_id: GoogleReportIdCreate) -> None:
        google_doc_id_dto = jsonable_encoder(google_report_id)
        existed_google_doc_id = await GoogleReportIdModel.get_by_month(
            month=google_report_id.month,
            report_type=google_report_id.report_type,
        )
        if existed_google_doc_id is None:
            await GoogleReportIdModel.create(**google_doc_id_dto)
            return

        await existed_google_doc_id.update(**google_doc_id_dto)


def get_report_config_service():
    return ReportConfigService()
