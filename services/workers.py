import logging
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import gspread
import gspread_formatting as gf
from fastapi import HTTPException

from core.settings import settings
from db.mssql import MsSqlDatabase
from legacy import functions
from legacy.barsicreport2 import BarsicReport2Service, get_legacy_service
from legacy.to_google_sheets import get_letter_column_name
from repositories.google import GoogleRepository, get_google_repo
from repositories.yandex import YandexRepository, get_yandex_repo
from schemas.bars import TotalReport
from schemas.google_report_ids import GoogleReportIdCreate
from schemas.report_cache import ReportCacheCreate
from schemas.total_report import DBName
from services.bars import BarsService, get_bars_service
from services.report_config import ReportConfigService, get_report_config_service
from services.reports import ReportService, get_report_service
from services.rk import RKService, get_rk_service

logger = logging.getLogger(__name__)


class WorkerService:
    def __init__(
        self,
        bars_srv: MsSqlDatabase,
        bars_service: BarsService,
        rk_service: RKService,
        report_config_service: ReportConfigService,
        legacy_service: BarsicReport2Service,
        report_service: ReportService,
        yandex_repo: YandexRepository,
        google_repo: GoogleRepository,
    ):
        self._bars_srv = bars_srv
        self._bars_service = bars_service
        self._rk_service = rk_service
        self._report_config_service = report_config_service
        self._legacy_service = legacy_service
        self._report_service = report_service
        self._yandex_repo = yandex_repo
        self._google_repo = google_repo

    def choose_db(self, db_name: str):
        self._bars_service.choose_db(db_name)

    async def get_total_report_with_groups(
        self,
        date_from: datetime,
        date_to: datetime,
        use_cache: bool = True,
    ) -> dict:
        # total_report = self._bars_service.get_total_report(
        #     organization_id=63,
        #     date_from=date_from,
        #     date_to=date_to,
        #     hide_zeroes=True,
        #     hide_internal=True,
        #     hide_discount=True,
        # )

        total_detail_full_report = defaultdict()

        date_from, date_to = self._period_cutting(date_from, date_to)

        logger.info(f"Try build total by day report from {date_from} to {date_to}")
        current_date = date_from
        while current_date < date_to and (
            current_date.month == date_to.month or current_date + timedelta(days=1) == date_to
        ):
            report_type = "total_detail"

            if use_cache:
                total_detail_report = await self._report_service.get_report_by_date(report_type, current_date.date())
            else:
                total_detail_report = None
                await self._report_service.delete_report(report_type, current_date.date())

            if total_detail_report is None:
                smile_report_month = self._rk_service.get_smile_report(
                    date_from=current_date,
                    date_to=current_date + timedelta(days=1),
                )
                total_report_config = await self._report_config_service.get_report_elements_with_groups("ItogReport")
                fin_report_config = await self._report_config_service.get_report_elements_with_groups("GoogleReport")
                org_list1 = self._legacy_service.list_organisation(
                    database=settings.mssql_database1,
                )
                for org in org_list1:
                    if org[0] == 36:
                        org1 = (org[0], org[2])

                self._bars_srv.set_database(settings.mssql_database1)
                with self._bars_srv as connect:
                    self._legacy_service.itog_report_month = functions.get_total_report(
                        connect=connect,
                        org=org1[0],
                        org_name=org1[1],
                        date_from=current_date,
                        date_to=current_date + timedelta(days=1),
                    )

                self._legacy_service.smile_report_month = smile_report_month
                month_finance_report = functions.create_month_finance_report(
                    itog_report_month=self._legacy_service.itog_report_month,
                    total_report_config=total_report_config,
                    fin_report_config=fin_report_config,
                    smile_report_month=self._legacy_service.smile_report_month,
                )
                total_detail_report = ReportCacheCreate(
                    report_date=current_date.date(),
                    report_type=report_type,
                    report_data=month_finance_report,
                )

                await self._report_service.save_report(total_detail_report)

            for (
                general_group,
                general_group_content,
            ) in total_detail_report.report_data.items():
                full_general_group_content = total_detail_full_report.setdefault(general_group, defaultdict())

                for group_name, group_content in general_group_content.items():
                    full_group_content = full_general_group_content.setdefault(group_name, [])

                    for group_data in group_content:
                        wrote = False
                        for full_group_data in full_group_content:
                            if group_data[0] == full_group_data[0]:
                                full_group_data[1][current_date.day] = group_data[1]
                                full_group_data[2][current_date.day] = group_data[2]
                                wrote = True

                        if not wrote:
                            full_group_content.append(
                                [
                                    group_data[0],
                                    {current_date.day: group_data[1]},
                                    {current_date.day: group_data[2]},
                                ]
                            )

            current_date += timedelta(days=1)

        gc = gspread.service_account_from_dict(settings.google_api_settings.google_service_account_config)

        months = [
            "",
            "Январь",
            "Февраль",
            "Март",
            "Апрель",
            "Май",
            "Июнь",
            "Июль",
            "Август",
            "Сентябрь",
            "Октябрь",
            "Ноябрь",
            "Декабрь",
        ]
        short_date = date_from.strftime("%Y-%m")
        report_name = "Итоговый отчет в разрезе дня"
        detail_name = f"за {months[date_from.month]} {date_from.year}"
        google_doc_id = await self._report_config_service.get_total_detail_doc_id_by_date(date_from)
        if google_doc_id is not None:
            google_doc = gc.open_by_key(google_doc_id.doc_id)
        else:
            google_doc = gc.create(f"{report_name} {detail_name}")

            for email in settings.google_api_settings.google_writer_list.split(","):
                if not email:
                    continue

                google_doc.share(email, "user", "writer")

            for email in settings.google_api_settings.google_reader_list.split(","):
                if not email:
                    continue

                google_doc.share(email, "user", "reader")

            await self._report_config_service.add_google_report_id(
                google_report_id=GoogleReportIdCreate(
                    month=short_date,
                    doc_id=google_doc.id,
                    report_type="total_detail",
                    version="1",
                )
            )

        worksheet = google_doc.get_worksheet(0)
        worksheet.clear()

        days_in_month = monthrange(date_from.year, date_from.month)[1]
        report_matrix = []
        total_line = None
        h2_lines = []
        h3_lines = []
        for general_group, general_group_content in total_detail_full_report.items():
            if general_group in ("Контрольная сумма", "Дата"):
                continue

            _, _, amounts = general_group_content["Итого по группе"][0]
            amounts_matrix = [amounts.get(day, "") for day in range(1, days_in_month + 1)]
            if general_group == "ИТОГО":
                total_line = [general_group, *amounts_matrix]
                continue

            report_matrix.append([general_group, *amounts_matrix])
            h2_lines.append(len(report_matrix) + 4)

            filtered_general_group_content = {k: v for k, v in general_group_content.items() if k is not None}
            for group_name, group_content in sorted(filtered_general_group_content.items()):
                if group_name in ("Итого по группе", "None", ""):
                    continue

                for group_data in group_content:
                    tariff_name, _, amounts = group_data
                    amounts_matrix = [amounts.get(day, "") for day in range(1, days_in_month + 1)]
                    if tariff_name == "Итого по папке":
                        report_matrix.append([group_name, *amounts_matrix])
                        h3_lines.append(len(report_matrix) + 4)
                    elif tariff_name in ("Итого по отчету",):
                        pass
                    else:
                        report_matrix.append([tariff_name, *amounts_matrix])

        # Добавление ИТОГО
        report_matrix.append(total_line)
        h2_lines.append(len(report_matrix) + 4)

        table_width = days_in_month + 1
        table_width_letter = get_letter_column_name(table_width)
        table_height = len(report_matrix) + 3

        worksheet.update([[report_name]], "A1")
        worksheet.update([[detail_name]], "A2")
        worksheet.update([["Услуги / Дни", *range(1, table_width)]], "A4")
        worksheet.update(report_matrix, "A5")

        border = gf.Border(style="SOLID")
        h1_fmt = gf.CellFormat(
            textFormat=gf.TextFormat(fontSize=18, bold=True),
            horizontalAlignment="LEFT",
        )
        h4_fmt = gf.CellFormat(
            textFormat=gf.TextFormat(fontSize=10, bold=False),
            horizontalAlignment="LEFT",
        )
        table_head_fmt = gf.CellFormat(
            backgroundColor=gf.Color.fromHex("#f7cb4d"),
            textFormat=gf.TextFormat(fontSize=14, bold=True),
            horizontalAlignment="CENTER",
            borders=gf.Borders(top=border, right=border, bottom=border, left=border),
        )
        table_h2_fmt = gf.CellFormat(
            backgroundColor=gf.Color.fromHex("#fce8b2"),
            textFormat=gf.TextFormat(fontSize=12, bold=True),
            horizontalAlignment="LEFT",
            borders=gf.Borders(top=border, right=border, bottom=border, left=border),
        )
        table_h3_fmt = gf.CellFormat(
            backgroundColor=gf.Color.fromHex("#fef8e3"),
            textFormat=gf.TextFormat(fontSize=10, bold=True),
            horizontalAlignment="LEFT",
            borders=gf.Borders(top=border, right=border, bottom=border, left=border),
        )
        body_fmt = gf.CellFormat(
            backgroundColor=gf.Color.fromHex("#ffffff"),
            textFormat=gf.TextFormat(fontSize=10, bold=False),
            horizontalAlignment="LEFT",
            borders=gf.Borders(top=border, right=border, bottom=border, left=border),
        )
        currency_fmt = gf.CellFormat(
            numberFormat=gf.NumberFormat(type="CURRENCY", pattern="#0[$ ₽]"),
        )
        clear_fmt = gf.CellFormat(backgroundColor=gf.Color.fromHex("#ffffff"))
        gf.format_cell_ranges(
            worksheet,
            [
                (f"A1:{table_width_letter}{table_height}", clear_fmt),
                ("A1:A1", h1_fmt),
                ("A2:A2", h4_fmt),
                (f"A4:{table_width_letter}4", table_head_fmt),
                (f"A5:{table_width_letter}{max(table_height, 5)}", body_fmt),
                (f"B5:{table_width_letter}{max(table_height, 5)}", currency_fmt),
                *[(f"A{line}:{table_width_letter}{line}", table_h2_fmt) for line in h2_lines],
                *[(f"A{line}:{table_width_letter}{line}", table_h3_fmt) for line in h3_lines],
            ],
        )
        worksheet.columns_auto_resize(0, table_width)

        return {"ok": True, "Google Report": google_doc.url}

    async def create_purchased_goods_report(
        self,
        date_from: datetime,
        date_to: datetime,
        goods: list[str],
        use_like: bool,
        save_to_yandex: bool,
        hide_zero: bool,
    ) -> dict:
        extended_services_report = self._bars_service.get_loan_transactions_by_service_names(
            date_from=date_from,
            date_to=date_to,
            service_names=goods,
            use_like=use_like,
        )
        report_path = self._yandex_repo.save_purchased_goods_report(
            report=extended_services_report,
            date_from=date_from,
            date_to=date_to,
            goods=goods,
            hide_zero=hide_zero,
        )
        result = {"ok": True, "local_path": report_path}
        if save_to_yandex:
            links = self._yandex_repo.sync_to_yadisk([report_path], date_from)
            link = links[0].publish().get_meta()
            return {
                **result,
                "public_url": link.public_url,
                "download_link": link.get_download_link(),
            }

        return result

    def _create_report_period(
        self, date_from: datetime, date_to: datetime, use_cache: bool = False
    ) -> list[tuple[datetime, bool]]:
        """Create a list of tuples representing the report period with cache usage flags."""

        date_from, date_to = self._period_cut_to_one_month(date_from, date_to)
        period = []
        date_from_month_max_day = monthrange(date_from.year, date_from.month)[1]
        last_day = date_from_month_max_day + 1 if date_to.month > date_from.month else date_to.day
        for _date in range(1, last_day):
            is_use_cache = use_cache if _date >= date_from.day else True
            period.append((datetime(date_from.year, date_from.month, _date), is_use_cache))

        return period

    async def create_attendance_report(
        self,
        date_from: datetime,
        date_to: datetime,
        save_to_yandex: bool,
        save_to_google: bool,
        use_cache: bool = True,
    ) -> dict:
        attendance_report = {}
        await self._check_undistributed_services(report_name="attendance")

        logger.info(f"Building attendance report from {date_from} to {date_to}...")
        period = self._create_report_period(date_from, date_to, use_cache=use_cache)
        for current_date, is_use_cache in period:
            report_type = "attendance"
            if is_use_cache:
                current_attendance_report = await self._report_service.get_report_by_date(
                    report_type, current_date.date()
                )
            else:
                current_attendance_report = None
                await self._report_service.delete_report(report_type, current_date.date())

            if current_attendance_report is None:
                report_config = await self._report_config_service.get_report_tree(report_type)
                companies = [
                    company for company in self._legacy_service.get_companies() if company.db_name == DBName.AQUA
                ]

                total_report = None
                for company in companies:
                    company_total_report = self._bars_service.get_total_report(
                        organization_id=company.id,
                        date_from=current_date,
                        date_to=current_date + timedelta(days=1),
                        hide_zeroes=False,
                        hide_internal=True,
                        hide_discount=False,
                    )
                    if total_report is None:
                        total_report = company_total_report
                    else:
                        total_report += company_total_report

                customer_count = self._bars_service.get_customer_count(
                    date_from=current_date, date_to=current_date + timedelta(days=1)
                )

                report_data = self._create_attendance_report(
                    total_report=total_report,
                    report_config=report_config,
                    customer_count=customer_count,
                )
                current_attendance_report = ReportCacheCreate(
                    report_date=current_date.date(),
                    report_type=report_type,
                    report_data=report_data,
                )
                await self._report_service.save_report(current_attendance_report)
            attendance_report[current_date.date()] = current_attendance_report

        report_path = self._yandex_repo.save_attendance_report(
            report=attendance_report,
            date_from=period[0][0],
            date_to=period[-1][0] + timedelta(days=1),
        )

        result = {
            "ok": True,
            "local_path": report_path,
            "yandex_public_url": None,
            "yandex_download_link": None,
            "google_report": None,
        }

        if save_to_yandex:
            links = self._yandex_repo.sync_to_yadisk([report_path], date_from)
            link = links[0].publish().get_meta()
            result.update(
                {
                    "yandex_public_url": link.public_url,
                    "yandex_download_link": link.get_download_link(),
                }
            )

        if save_to_google:
            google_report = await self._get_cached_attendance_report_for_month(
                date_from=date_from,
                report_type="attendance",
                report=attendance_report,
            )
            existed_google_report = await self._report_config_service.get_attendance_doc_id_by_date(date_from)
            report_path, google_doc_id = self._google_repo.save_attendance_report(
                report=google_report,
                date_from=period[0][0],
                date_to=period[-1][0] + timedelta(days=1),
                google_doc_id=existed_google_report.doc_id if existed_google_report is not None else None,
            )
            await self._report_config_service.save_google_report_id(
                GoogleReportIdCreate(
                    month=date_from.strftime("%Y-%m"),
                    doc_id=google_doc_id,
                    report_type="attendance",
                    version=1,
                )
            )
            result.update(
                {
                    "google_report": report_path,
                }
            )

        return result

    async def _check_undistributed_services(self, report_name: str) -> None:
        all_tariffs = []
        organizations = self._bars_service.get_organisations()
        for organization in organizations:
            organization_tariffs = self._bars_service.get_tariffs(organization.super_account_id)
            all_tariffs.extend([tariff.name for tariff in organization_tariffs])

        all_tariffs.append("Смайл")
        distributed_tariffs = await self._report_config_service.get_report_elements(report_name)
        distributed_tariff_names = {tariff.title for tariff in distributed_tariffs}
        new_tariffs = sorted(set(all_tariffs) - distributed_tariff_names)

        if new_tariffs:
            error_message = f"Найдены нераспределенные тарифы в отчете {report_name}: {new_tariffs}"
            logger.error(error_message)
            raise HTTPException(
                status_code=409,
                detail=error_message,
            )

    async def _get_cached_attendance_report_for_month(
        self,
        date_from: datetime,
        report_type: str,
        report: dict[date, Any],
    ) -> dict[date, Any]:
        month_report: dict[date, Any] = {}
        days_in_month = monthrange(date_from.year, date_from.month)[1]
        for day in range(1, days_in_month + 1):
            current_day = date(date_from.year, date_from.month, day)
            day_report = report.get(current_day)
            if day_report is None:
                day_report = await self._report_service.get_report_by_date(report_type, current_day)
            if day_report is not None:
                month_report[current_day] = day_report

        return month_report

    def _create_attendance_report(
        self,
        total_report: TotalReport,
        report_config: dict[str, Any],
        customer_count: int,
    ):
        """Создает отчет по посещаемости."""

        total_report_map = {el.name: el for el in total_report.elements}

        result = {}
        for h1_header, h2_headers in report_config.items():
            h1 = result.setdefault(h1_header, {})
            for h2_header, h3_headers in h2_headers.items():
                h2 = h1.setdefault(h2_header, {})
                for h3_header, elements in h3_headers.items():
                    h2.setdefault(h3_header, 0)
                    for element in elements:
                        if total_report_map.get(element):
                            h2[h3_header] += total_report_map[element].good_amount

        result.setdefault("Количество посещений", {}).setdefault(
            "Количество посещений / Количество посещений", {}
        ).setdefault("Количество посещений / Количество посещений / Количество посещений", customer_count)

        return result

    @staticmethod
    def _period_cutting(date_from: datetime, date_to: datetime) -> tuple[datetime, datetime]:
        """Cut date_to to the end of a date_from month if date_from and date_to are in different months,
        because the report is built by month
        """
        if date_from.month == date_to.month - 1:
            days_in_month = monthrange(date_from.year, date_from.month)[1]
            date_to = datetime.combine(
                date(date_from.year, date_from.month, days_in_month),
                datetime.max.time(),
            )
        elif date_from.month == date_to.month:
            pass
        else:
            raise HTTPException(
                status_code=400,
                detail="date_from and date_to should be in the same month or in adjacent months",
            )

        return date_from, date_to

    @staticmethod
    def _period_cut_to_one_month(date_from: datetime, date_to: datetime) -> tuple[datetime, datetime]:
        """Cut date_to to the end of a date_from month if date_from and date_to are in different months,
        because the report is built by month
        """
        if date_to.month > date_from.month:
            days_in_month = monthrange(date_from.year, date_from.month)[1]
            date_to = datetime(date_from.year, date_from.month, days_in_month) + timedelta(days=1)

        return date_from, date_to


def get_worker_service() -> WorkerService:
    bars_srv = MsSqlDatabase(
        server=settings.mssql_server,
        user=settings.mssql_user,
        password=settings.mssql_pwd,
    )
    return WorkerService(
        bars_srv=bars_srv,
        bars_service=get_bars_service(),
        rk_service=get_rk_service(),
        report_config_service=get_report_config_service(),
        legacy_service=get_legacy_service(),
        report_service=get_report_service(),
        yandex_repo=get_yandex_repo(),
        google_repo=get_google_repo(),
    )
