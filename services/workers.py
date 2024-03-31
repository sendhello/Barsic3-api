import logging
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta

import gspread
import gspread_formatting as gf
from fastapi import HTTPException

from core.settings import settings
from legacy.barsicreport2 import BarsicReport2Service, get_legacy_service
from legacy.to_google_sheets import get_letter_column_name
from schemas.google_report_ids import GoogleReportIdCreate
from schemas.report_cache import ReportCacheCreate
from services.bars import BarsService, get_bars_service
from services.report_config import ReportConfigService, get_report_config_service
from services.reports import ReportService, get_report_service


logger = logging.getLogger(__name__)


class WorkerService:
    def __init__(
        self,
        bars_service: BarsService,
        report_config_service: ReportConfigService,
        legacy_service: BarsicReport2Service,
        report_service: ReportService,
    ):
        self._bars_service = bars_service
        self._report_config_service = report_config_service
        self._legacy_service = legacy_service
        self._report_service = report_service

    def choose_db(self, db_name: str):
        self._bars_service.choose_db(db_name)

    async def get_total_report_with_groups(
        self, date_from: datetime, date_to: datetime
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

        if date_from >= date_to:
            raise HTTPException(status_code=404, detail="date_from >= date_to")

        if date_from.month == date_to.month - 1:
            days_in_month = monthrange(date_from.year, date_from.month)[1]
            date_to = datetime.combine(
                date(date_from.year, date_from.month, days_in_month),
                datetime.max.time(),
            )

        logger.info(f"Try build total by day report from {date_from} to {date_to}")
        current_date = date_from
        while current_date < date_to and (
            current_date.month == date_to.month
            or current_date + timedelta(days=1) == date_to
        ):
            report_type = "total_detail"
            total_detail_report = await self._report_service.get_report_by_date(
                report_type, current_date.date()
            )
            if total_detail_report is None:
                report_rk_month = self._legacy_service.rk_report_request(
                    server=settings.mssql_server_rk,
                    database=settings.mssql_database_rk,
                    user=settings.mssql_user_rk,
                    password=settings.mssql_pwd_rk,
                    cash_id=15033,
                    date_from=current_date,
                    date_to=current_date + timedelta(days=1),
                )
                itogreport_group_dict = (
                    await self._report_config_service.get_report_elements_with_groups(
                        "ItogReport"
                    )
                )
                self._legacy_service.orgs_dict = (
                    await self._report_config_service.get_report_elements_with_groups(
                        "GoogleReport"
                    )
                )
                org_list1 = self._legacy_service.list_organisation(
                    server=settings.mssql_server,
                    database=settings.mssql_database1,
                    user=settings.mssql_user,
                    password=settings.mssql_pwd,
                )
                for org in org_list1:
                    if org[0] == 36:
                        org1 = (org[0], org[2])

                self._legacy_service.itog_report_month = (
                    self._legacy_service.itog_report(
                        server=settings.mssql_server,
                        database=settings.mssql_database1,
                        user=settings.mssql_user,
                        password=settings.mssql_pwd,
                        org=org1[0],
                        org_name=org1[1],
                        date_from=current_date,
                        date_to=current_date + timedelta(days=1),
                    )
                )

                self._legacy_service.report_rk_month = report_rk_month
                self._legacy_service.itogreport_group_dict = itogreport_group_dict
                total_detail_report_data = self._legacy_service.fin_report_month()
                total_detail_report = ReportCacheCreate(
                    report_date=current_date.date(),
                    report_type=report_type,
                    report_data=total_detail_report_data,
                )

                await self._report_service.save_report(total_detail_report)

            for (
                general_group,
                general_group_content,
            ) in total_detail_report.report_data.items():
                full_general_group_content = total_detail_full_report.setdefault(
                    general_group, defaultdict()
                )

                for group_name, group_content in general_group_content.items():
                    full_group_content = full_general_group_content.setdefault(
                        group_name, []
                    )

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

        gc = gspread.service_account_from_dict(
            settings.google_api_settings.google_service_account_config
        )

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
        google_doc_id = (
            await self._report_config_service.get_total_detail_doc_id_by_date(date_from)
        )
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
            amounts_matrix = [
                amounts.get(day, "") for day in range(1, days_in_month + 1)
            ]
            if general_group == "ИТОГО":
                total_line = [general_group, *amounts_matrix]
                continue

            report_matrix.append([general_group, *amounts_matrix])
            h2_lines.append(len(report_matrix) + 4)

            for group_name, group_content in general_group_content.items():
                if group_name in ("Итого по группе", "None", ""):
                    continue

                for group_data in group_content:
                    tariff_name, _, amounts = group_data
                    amounts_matrix = [
                        amounts.get(day, "") for day in range(1, days_in_month + 1)
                    ]
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
        logger.warning(f"{table_height=}")
        gf.format_cell_ranges(
            worksheet,
            [
                (f"A1:{table_width_letter}{table_height}", clear_fmt),
                ("A1:A1", h1_fmt),
                ("A2:A2", h4_fmt),
                (f"A4:{table_width_letter}4", table_head_fmt),
                (f"A5:{table_width_letter}{max(table_height, 5)}", body_fmt),
                (f"B5:{table_width_letter}{max(table_height, 5)}", currency_fmt),
                *[
                    (f"A{line}:{table_width_letter}{line}", table_h2_fmt)
                    for line in h2_lines
                ],
                *[
                    (f"A{line}:{table_width_letter}{line}", table_h3_fmt)
                    for line in h3_lines
                ],
            ],
        )
        worksheet.columns_auto_resize(0, table_width)

        return {"ok": True, "Google Report": google_doc.url}


def get_worker_service():
    return WorkerService(
        bars_service=get_bars_service(),
        report_config_service=get_report_config_service(),
        legacy_service=get_legacy_service(),
        report_service=get_report_service(),
    )
