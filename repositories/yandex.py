import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal

from fastapi.exceptions import HTTPException
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from starlette import status
from yadisk import YaDisk
from yadisk.objects.resources import SyncResourceLinkObject

from core.settings import settings


logger = logging.getLogger(__name__)


class ReportStyle:
    h1 = Font(
        name="Times New Roman",
        size=18,
        bold=True,
        italic=False,
        vertAlign=None,
        underline="none",
        strike=False,
        color="FF000000",
    )
    h2 = Font(
        name="Times New Roman",
        size=14,
        bold=True,
        italic=False,
        vertAlign=None,
        underline="none",
        strike=False,
        color="FF000000",
    )
    h3 = Font(
        name="Times New Roman",
        size=11,
        bold=True,
        italic=False,
        vertAlign=None,
        underline="none",
        strike=False,
        color="FF000000",
    )
    font = Font(
        name="Times New Roman",
        size=9,
        bold=False,
        italic=False,
        vertAlign=None,
        underline="none",
        strike=False,
        color="FF000000",
    )
    font_bold = Font(
        name="Times New Roman",
        size=9,
        bold=True,
        italic=False,
        vertAlign=None,
        underline="none",
        strike=False,
        color="FF000000",
    )
    font_red = Font(
        name="Times New Roman",
        size=9,
        bold=True,
        italic=False,
        vertAlign=None,
        underline="none",
        strike=False,
        color="FFFF0000",
    )
    align_bottom = Alignment(
        horizontal="general",
        vertical="bottom",
        text_rotation=0,
        wrap_text=False,
        shrink_to_fit=False,
        indent=0,
    )
    border = Border(
        left=Side(border_style="thin", color="FF000000"),
        right=Side(border_style="thin", color="FF000000"),
        top=Side(border_style="thin", color="FF000000"),
        bottom=Side(border_style="thin", color="FF000000"),
        diagonal=Side(border_style="thin", color="FF000000"),
        diagonal_direction=0,
        outline=Side(border_style="thin", color="FF000000"),
        vertical=Side(border_style="thin", color="FF000000"),
        horizontal=Side(border_style="thin", color="FF000000"),
    )
    border_top_bottom = Border(
        bottom=Side(border_style="thin", color="FF000000"),
        top=Side(border_style="thin", color="FF000000"),
    )
    border_right = Border(right=Side(border_style="thin", color="FF000000"))
    border_left = Border(left=Side(border_style="thin", color="FF000000"))
    border_top = Border(top=Side(border_style="thin", color="FF000000"))
    border_left_top = Border(
        top=Side(border_style="thin", color="FF000000"),
        left=Side(border_style="thin", color="FF000000"),
    )
    border_right_top = Border(
        top=Side(border_style="thin", color="FF000000"),
        right=Side(border_style="thin", color="FF000000"),
    )
    fill = PatternFill(fill_type="solid", start_color="c1c1c1", end_color="c2c2c2")
    align_top = Alignment(
        horizontal="general",
        vertical="top",
        text_rotation=0,
        wrap_text=False,
        shrink_to_fit=False,
        indent=0,
    )
    align_left = Alignment(
        horizontal="left",
        vertical="bottom",
        text_rotation=0,
        wrap_text=False,
        shrink_to_fit=False,
        indent=0,
    )


class YandexRepository:
    def __init__(self):
        pass

    def save_purchased_goods_report(
        self,
        report,
        date_from,
        date_to,
        goods: list[str] = None,
        hide_zero=False,
    ):
        """Сохраняет отчет по количеству клиентов за день в Excel"""

        column = ["", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"]
        self.row = "0"

        def next_row():
            self.row = str(int(self.row) + 1)
            return self.row

        # объект
        wb = Workbook()

        # активный лист
        ws = wb.active

        # название страницы
        # ws = wb.create_sheet('первая страница', 0)
        ws.title = "Суммы трат в аквазоне"
        # шрифты
        ws["C1"].font = ReportStyle.h1
        # выравнивание
        ws["C1"].alignment = ReportStyle.align_left

        # Ширина стролбцов
        ws.column_dimensions["A"].width = 1 / 7 * 8
        ws.column_dimensions["B"].width = 1 / 7 * 8
        ws.column_dimensions["C"].width = 1 / 7 * 80
        ws.column_dimensions["D"].width = 1 / 7 * 8
        ws.column_dimensions["E"].width = 1 / 7 * 88
        ws.column_dimensions["F"].width = 1 / 7 * 8
        ws.column_dimensions["G"].width = 1 / 7 * 24
        ws.column_dimensions["H"].width = 1 / 7 * 8
        ws.column_dimensions["I"].width = 1 / 7 * 80
        ws.column_dimensions["J"].width = 1 / 7 * 8
        ws.column_dimensions["K"].width = 1 / 7 * 144
        ws.column_dimensions["L"].width = 1 / 7 * 144
        ws.column_dimensions["M"].width = 1 / 7 * 8

        # значение ячейки
        # ws['A1'] = "Hello!"

        ws[column[3] + next_row()] = "Отчет по купленным товарам"
        ws.merge_cells(
            start_row=self.row, start_column=3, end_row=self.row, end_column=12
        )
        ws[column[1] + next_row()] = ""
        ws[column[3] + next_row()] = f"По товарам: {', '.join(goods)}"
        ws.merge_cells(
            start_row=self.row, start_column=3, end_row=self.row, end_column=12
        )
        ws[column[3] + self.row].font = ReportStyle.font
        ws[column[3] + self.row].alignment = ReportStyle.align_top
        ws[column[1] + next_row()] = ""

        if date_from == date_to - timedelta(days=1):
            ws[column[3] + next_row()] = "За:"
            ws[column[3] + self.row].font = ReportStyle.font
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            ws[column[5] + self.row] = date_from.strftime("%d.%m.%Y")
            ws[column[5] + self.row].font = ReportStyle.font_bold
            ws[column[5] + self.row].alignment = ReportStyle.align_top
        else:
            ws[column[3] + next_row()] = "За период с:"
            ws[column[3] + self.row].font = ReportStyle.font
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            ws[column[5] + self.row] = date_from.strftime("%d.%m.%Y")
            ws[column[5] + self.row].font = ReportStyle.font_bold
            ws[column[5] + self.row].alignment = ReportStyle.align_top
            ws[column[7] + self.row] = "По:"
            ws[column[7] + self.row].font = ReportStyle.font
            ws[column[7] + self.row].alignment = ReportStyle.align_top
            ws[column[9] + self.row] = (date_to - timedelta(days=1)).strftime(
                "%d.%m.%Y"
            )
            ws[column[9] + self.row].font = ReportStyle.font_bold
            ws[column[9] + self.row].alignment = ReportStyle.align_top

        # ТАБЛИЦА
        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=10, end_row=self.row, end_column=11
            )
            ws.merge_cells(
                start_row=self.row, start_column=12, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.font
            ws[column[10] + self.row].font = ReportStyle.font
            ws[column[12] + self.row].font = ReportStyle.font
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[12] + self.row].alignment = ReportStyle.align_top
            b = 2
            while b <= 13:
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        def merge_table_h3():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=10, end_row=self.row, end_column=11
            )
            ws.merge_cells(
                start_row=self.row, start_column=12, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h3
            ws[column[10] + self.row].font = ReportStyle.h3
            ws[column[12] + self.row].font = ReportStyle.h3
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[12] + self.row].alignment = ReportStyle.align_top
            ws[column[2] + self.row].border = ReportStyle.border_left
            ws[column[13] + self.row].border = ReportStyle.border_right

        def merge_table_h2():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=10, end_row=self.row, end_column=11
            )
            ws.merge_cells(
                start_row=self.row, start_column=12, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h2
            ws[column[10] + self.row].font = ReportStyle.h2
            ws[column[12] + self.row].font = ReportStyle.h2
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[12] + self.row].alignment = ReportStyle.align_top
            ws[column[2] + self.row].border = ReportStyle.border_left
            ws[column[13] + self.row].border = ReportStyle.border_right

        def merge_width_h2():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h2
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            b = 2
            while b <= 13:
                if b == 2:
                    ws[column[b] + self.row].border = ReportStyle.border_left_top
                elif b == 13:
                    ws[column[b] + self.row].border = ReportStyle.border_right_top
                else:
                    ws[column[b] + self.row].border = ReportStyle.border_top
                b += 1

        def merge_width_h3():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h3
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            b = 2
            while b <= 13:
                if b == 2:
                    ws[column[b] + self.row].border = ReportStyle.border_left
                elif b == 13:
                    ws[column[b] + self.row].border = ReportStyle.border_right
                b += 1

        ws[column[2] + next_row()] = "Наименование"
        ws[column[10] + self.row] = "Количество"
        ws[column[12] + self.row] = "Сумма"
        merge_table()
        ws[column[2] + self.row].font = ReportStyle.h3
        ws[column[10] + self.row].font = ReportStyle.h3
        ws[column[12] + self.row].font = ReportStyle.h3
        ws[column[2] + self.row].alignment = ReportStyle.align_top
        ws[column[10] + self.row].alignment = ReportStyle.align_top
        ws[column[12] + self.row].alignment = ReportStyle.align_top

        for line in report:
            if hide_zero and line.summ == Decimal(0):
                continue

            if line.name in goods:
                continue

            ws[column[2] + next_row()] = (
                line.name[7:] if line.name.startswith("Долг за") else line.name
            )
            ws[column[10] + self.row] = line.count
            ws[column[12] + self.row] = line.summ
            ws[column[12] + self.row].number_format = "#,##0.00 ₽"
            merge_table()

        ws[column[2] + next_row()] = "Итого"
        ws[column[10] + self.row] = sum(
            line.count for line in report if line.name not in goods
        )
        ws[column[12] + self.row] = sum(
            line.summ for line in report if line.name not in goods
        )
        ws[column[12] + self.row].number_format = "#,##0.00 ₽"
        merge_table_h2()
        ws[column[2] + self.row].alignment = ReportStyle.align_bottom
        ws[column[10] + self.row].alignment = ReportStyle.align_bottom
        ws[column[12] + self.row].alignment = ReportStyle.align_bottom
        b = 2
        while b <= 13:
            ws[column[b] + self.row].border = ReportStyle.border_top_bottom
            b += 1
        end_line = int(self.row)

        # раскрвшивание фона для заголовков
        i = 2
        while i <= 13:
            ws[column[i] + "6"].fill = ReportStyle.fill
            i += 1

        # обводка
        # ws['A3'].border = ReportStyle.border

        # вручную устанавливаем высоту первой строки
        rd = ws.row_dimensions[1]
        rd.height = 21.75

        # увеличиваем все строки по высоте
        max_row = ws.max_row
        i = 2
        while i <= max_row:
            rd = ws.row_dimensions[i]
            rd.height = 18
            i += 1

        # Высота строк
        ws.row_dimensions[2].height = 5.25
        ws.row_dimensions[4].height = 6.75
        ws.row_dimensions[end_line].height = 30.75

        # выравнивание столбца
        for cellObj in ws["A2:A5"]:
            for cell in cellObj:
                ws[cell.coordinate].alignment = ReportStyle.align_left

        # перетягивание ячеек
        # https://stackoverflow.com/questions/13197574/openpyxl-adjust-column-width-size
        # dims = {}
        # for row in ws.rows:
        #     for cell in row:
        #         if cell.value:
        #             dims[cell.column] = max((dims.get(cell.column, 0), len(cell.value)))
        # for col, value in dims.items():
        #     # value * коэфициент
        #     ws.column_dimensions[col].width = value * 1.5

        if date_from == date_to - timedelta(days=1):
            date_ = datetime.strftime(date_from, "%Y-%m-%d")
        else:
            date_ = (
                f'{datetime.strftime(date_from, "%Y-%m-%d")} - '
                f'{datetime.strftime(date_to - timedelta(days=1), "%Y-%m-%d")}'
            )
        path = (
            settings.local_folder
            + settings.report_path
            + date_
            + f" Отчет по купленным товарам ({datetime.now().timestamp()}).xlsx"
        )
        logger.info(f"Сохранение отчета по купленным товарам за {date_} в {path}")
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def export_payment_agent_report(self, report: dict[str, list], date_from: datetime):
        """Сохраняет отчет платежного агента в виде Excel-файла в локальную директорию"""

        table_color = PatternFill(
            fill_type="solid", start_color="e2e2e2", end_color="e9e9e9"
        )

        column = ["", "A", "B", "C", "D", "E"]

        self.row = "0"

        def next_row():
            self.row = str(int(self.row) + 1)
            return self.row

        # объект
        wb = Workbook()

        # активный лист
        ws = wb.active

        # название страницы
        # ws = wb.create_sheet('первая страница', 0)
        ws.title = "Отчет платежного агента"
        # шрифты
        ws["A1"].font = ReportStyle.h1
        # выравнивание
        ws["A1"].alignment = ReportStyle.align_left

        # Ширина стролбцов
        ws.column_dimensions["A"].width = 1 / 7 * 124
        ws.column_dimensions["B"].width = 1 / 7 * 80
        ws.column_dimensions["C"].width = 1 / 7 * 24
        ws.column_dimensions["D"].width = 1 / 7 * 175
        ws.column_dimensions["E"].width = 1 / 7 * 200

        # значение ячейки
        # ws['A1'] = "Hello!"

        ws[column[1] + next_row()] = (
            "Отчет платежного агента по приему денежных средств"
        )
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        # шрифты
        ws[column[1] + self.row].font = ReportStyle.h1
        # выравнивание
        ws[column[1] + self.row].alignment = ReportStyle.align_left
        # Высота строк
        ws.row_dimensions[1].height = 24

        ws[column[1] + next_row()] = f'{report["Организация"][1]}'
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        ws[column[1] + self.row].font = ReportStyle.font
        ws[column[1] + self.row].alignment = ReportStyle.align_top

        ws[column[1] + next_row()] = "За период с:"
        ws[column[1] + self.row].font = ReportStyle.font
        ws[column[1] + self.row].alignment = ReportStyle.align_top
        ws[column[2] + self.row] = (report["Дата"][0]).strftime("%d.%m.%Y")
        ws[column[2] + self.row].font = ReportStyle.font_bold
        ws[column[2] + self.row].alignment = ReportStyle.align_top
        ws[column[3] + self.row] = "по"
        ws[column[3] + self.row].font = ReportStyle.font
        ws[column[3] + self.row].alignment = ReportStyle.align_top
        ws[column[4] + self.row] = (report["Дата"][1] - timedelta(1)).strftime(
            "%d.%m.%Y"
        )
        ws[column[4] + self.row].font = ReportStyle.font_bold
        ws[column[4] + self.row].alignment = ReportStyle.align_top

        # ТАБЛИЦА
        self.color = False

        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=4
            )
            ws[column[1] + self.row].font = ReportStyle.font
            ws[column[5] + self.row].font = ReportStyle.font
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[5] + self.row].alignment = ReportStyle.align_top
            ws[column[5] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1
            if self.color:
                b = 1
                while b < len(column):
                    ws[column[b] + self.row].fill = table_color
                    b += 1
                self.color = False
            else:
                self.color = True

        def merge_table_bold():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=4
            )
            ws[column[1] + self.row].font = ReportStyle.font_bold
            ws[column[5] + self.row].font = ReportStyle.font_bold
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[5] + self.row].alignment = ReportStyle.align_top
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        ws[column[1] + next_row()] = "Наименование поставщика услуг"
        ws[column[5] + self.row] = "Сумма"
        merge_table_bold()
        # раскрашивание фона для заголовков
        b = 1
        while b < len(column):
            ws[column[b] + self.row].fill = ReportStyle.fill
            b += 1

        itog_sum = 0
        for line in report:
            if line != "Организация" and line != "Дата" and line != "ИТОГО":
                try:
                    itog_sum += report[line][1]
                    ws[column[1] + next_row()] = line
                    ws[column[5] + self.row] = report[line][1]
                    merge_table()
                except AttributeError:
                    pass

        ws[column[1] + next_row()] = "Итого"
        if itog_sum != report["ИТОГО"][1]:
            logger.error(
                f"Ошибка. Отчет платежного агента: сумма строк "
                f"({itog_sum}) не равна строке ИТОГО "
                f'({report["ИТОГО"][1]})'
            )
            logger.info(
                "Ошибка. Отчет платежного агента",
                "Ошибка. Отчет платежного агента: сумма строк "
                f"({itog_sum}) не равна строке ИТОГО "
                f'({report["ИТОГО"][1]})',
            )
        ws[column[5] + self.row] = itog_sum
        ws[column[5] + self.row].number_format = "#,##0.00 ₽"
        merge_table_bold()

        # увеличиваем все строки по высоте
        max_row = ws.max_row
        i = 2
        while i <= max_row:
            rd = ws.row_dimensions[i]
            rd.height = 18
            i += 1
        if report["Дата"][0] == report["Дата"][1] - timedelta(1):
            date_ = datetime.strftime(report["Дата"][0], "%Y-%m-%d")
        else:
            date_ = (
                f'{datetime.strftime(report["Дата"][0], "%Y-%m-%d")} - '
                f'{datetime.strftime(report["Дата"][1], "%Y-%m-%d")}'
            )
        path = (
            settings.local_folder
            + settings.report_path
            + date_
            + f' Отчет платежного агента {report["Организация"][1]}'
            + ".xlsx"
        )
        logger.info(
            f"Сохранение отчета платежного агента "
            f'{report["Организация"][1]} в {path}'
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def save_organisation_total(self, itog_report, date_from):
        """
        Сохраняет Итоговый отчет в Excel
        """
        organisation_total = {}
        for key in itog_report:
            organisation_total[itog_report[key][3]] = {}
        for key in itog_report:
            organisation_total[itog_report[key][3]][itog_report[key][2]] = []
        for key in itog_report:
            organisation_total[itog_report[key][3]][itog_report[key][2]].append(
                (key, itog_report[key][0], itog_report[key][1])
            )

        column = ["", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"]

        self.row = "0"

        def next_row():
            self.row = str(int(self.row) + 1)
            return self.row

        # объект
        wb = Workbook()

        # активный лист
        ws = wb.active

        # название страницы
        # ws = wb.create_sheet('первая страница', 0)
        ws.title = "Итоговый отчет"
        # шрифты
        ws["C1"].font = ReportStyle.h1
        # выравнивание
        ws["C1"].alignment = ReportStyle.align_left

        # Ширина стролбцов
        ws.column_dimensions["A"].width = 1 / 7 * 8
        ws.column_dimensions["B"].width = 1 / 7 * 8
        ws.column_dimensions["C"].width = 1 / 7 * 80
        ws.column_dimensions["D"].width = 1 / 7 * 8
        ws.column_dimensions["E"].width = 1 / 7 * 88
        ws.column_dimensions["F"].width = 1 / 7 * 8
        ws.column_dimensions["G"].width = 1 / 7 * 24
        ws.column_dimensions["H"].width = 1 / 7 * 8
        ws.column_dimensions["I"].width = 1 / 7 * 80
        ws.column_dimensions["J"].width = 1 / 7 * 8
        ws.column_dimensions["K"].width = 1 / 7 * 144
        ws.column_dimensions["L"].width = 1 / 7 * 144
        ws.column_dimensions["M"].width = 1 / 7 * 8

        # значение ячейки
        # ws['A1'] = "Hello!"

        ws[column[3] + next_row()] = "Итоговый отчет"
        ws.merge_cells(
            start_row=self.row, start_column=3, end_row=self.row, end_column=12
        )
        ws[column[1] + next_row()] = ""
        ws[column[3] + next_row()] = organisation_total["Организация"]["Организация"][
            0
        ][0]
        ws.merge_cells(
            start_row=self.row, start_column=3, end_row=self.row, end_column=12
        )
        ws[column[3] + self.row].font = ReportStyle.font
        ws[column[3] + self.row].alignment = ReportStyle.align_top
        ws[column[1] + next_row()] = ""

        ws[column[3] + next_row()] = "За период с:"
        ws[column[3] + self.row].font = ReportStyle.font
        ws[column[3] + self.row].alignment = ReportStyle.align_top
        ws[column[5] + self.row] = itog_report["Дата"][0].strftime("%d.%m.%Y")
        ws[column[5] + self.row].font = ReportStyle.font_bold
        ws[column[5] + self.row].alignment = ReportStyle.align_top
        ws[column[7] + self.row] = "По:"
        ws[column[7] + self.row].font = ReportStyle.font
        ws[column[7] + self.row].alignment = ReportStyle.align_top
        ws[column[9] + self.row] = (itog_report["Дата"][1] - timedelta(1)).strftime(
            "%d.%m.%Y"
        )
        ws[column[9] + self.row].font = ReportStyle.font_bold
        ws[column[9] + self.row].alignment = ReportStyle.align_top

        # ТАБЛИЦА
        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=10, end_row=self.row, end_column=11
            )
            ws.merge_cells(
                start_row=self.row, start_column=12, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.font
            ws[column[10] + self.row].font = ReportStyle.font
            ws[column[12] + self.row].font = ReportStyle.font
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[12] + self.row].alignment = ReportStyle.align_top
            b = 2
            while b <= 13:
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        def merge_table_h3():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=10, end_row=self.row, end_column=11
            )
            ws.merge_cells(
                start_row=self.row, start_column=12, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h3
            ws[column[10] + self.row].font = ReportStyle.h3
            ws[column[12] + self.row].font = ReportStyle.h3
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[12] + self.row].alignment = ReportStyle.align_top
            ws[column[2] + self.row].border = ReportStyle.border_left
            ws[column[13] + self.row].border = ReportStyle.border_right

        def merge_table_h2():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=10, end_row=self.row, end_column=11
            )
            ws.merge_cells(
                start_row=self.row, start_column=12, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h2
            ws[column[10] + self.row].font = ReportStyle.h2
            ws[column[12] + self.row].font = ReportStyle.h2
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[12] + self.row].alignment = ReportStyle.align_top
            ws[column[2] + self.row].border = ReportStyle.border_left
            ws[column[13] + self.row].border = ReportStyle.border_right

        def merge_width_h2():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h2
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            b = 2
            while b <= 13:
                if b == 2:
                    ws[column[b] + self.row].border = ReportStyle.border_left_top
                elif b == 13:
                    ws[column[b] + self.row].border = ReportStyle.border_right_top
                else:
                    ws[column[b] + self.row].border = ReportStyle.border_top
                b += 1

        def merge_width_h3():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = ReportStyle.h3
            ws[column[2] + self.row].alignment = ReportStyle.align_top
            b = 2
            while b <= 13:
                if b == 2:
                    ws[column[b] + self.row].border = ReportStyle.border_left
                elif b == 13:
                    ws[column[b] + self.row].border = ReportStyle.border_right
                b += 1

        ws[column[2] + next_row()] = "Название"
        ws[column[10] + self.row] = "Количество"
        ws[column[12] + self.row] = "Сумма"
        merge_table()
        ws[column[2] + self.row].font = ReportStyle.h3
        ws[column[10] + self.row].font = ReportStyle.h3
        ws[column[12] + self.row].font = ReportStyle.h3
        ws[column[2] + self.row].alignment = ReportStyle.align_top
        ws[column[10] + self.row].alignment = ReportStyle.align_top
        ws[column[12] + self.row].alignment = ReportStyle.align_top

        groups = [
            "Депозит",
            "Карты",
            "Услуги",
            "Товары",
            "Платные зоны",
        ]
        all_count = 0
        all_sum = 0
        try:
            for gr in groups:
                ws[column[2] + next_row()] = gr
                merge_width_h2()
                group_count = 0
                group_sum = 0
                organisation_total_groups = organisation_total.get(gr)
                if not organisation_total_groups:
                    continue
                for group in organisation_total_groups:
                    ws[column[2] + next_row()] = group
                    merge_width_h3()
                    service_count = 0
                    service_sum = 0
                    servises = organisation_total_groups.get(group, [])
                    if not servises:
                        continue
                    for service in servises:
                        try:
                            service_count += service[1]
                            service_sum += service[2]
                        except TypeError:
                            pass
                        ws[column[2] + next_row()] = service[0]
                        ws[column[10] + self.row] = service[1]
                        ws[column[12] + self.row] = service[2]
                        ws[column[12] + self.row].number_format = "#,##0.00 ₽"
                        merge_table()
                    ws[column[10] + next_row()] = service_count
                    ws[column[12] + self.row] = service_sum
                    ws[column[12] + self.row].number_format = "#,##0.00 ₽"
                    merge_table_h3()
                    group_count += service_count
                    group_sum += service_sum
                ws[column[10] + next_row()] = group_count
                ws[column[12] + self.row] = group_sum
                ws[column[12] + self.row].number_format = "#,##0.00 ₽"
                merge_table_h2()
                all_count += group_count
                all_sum += group_sum
                group_count = 0
                group_sum = 0
        except KeyError:
            pass

        bars_total_sum = organisation_total["Итого по отчету"][""][0]
        if all_sum == bars_total_sum[2]:
            ws[column[2] + next_row()] = bars_total_sum[0]
            ws[column[10] + self.row] = bars_total_sum[1]
            ws[column[12] + self.row] = bars_total_sum[2]
            self.total_report_sum = all_sum
        else:
            error_code = "Ошибка: Итоговые суммы не совпадают."
            error_message = (
                f'"Итого по отчету" из Барса ({bars_total_sum[2]})'
                f" не совпадает с итоговой суммой по формируемым строкам ({all_sum})."
            )
            logger.error(f"{error_code} {error_message}")
            return None

        ws[column[12] + self.row].number_format = "#,##0.00 ₽"
        merge_table_h2()
        ws[column[2] + self.row].alignment = ReportStyle.align_bottom
        ws[column[10] + self.row].alignment = ReportStyle.align_bottom
        ws[column[12] + self.row].alignment = ReportStyle.align_bottom
        b = 2
        while b <= 13:
            ws[column[b] + self.row].border = ReportStyle.border_top_bottom
            b += 1
        end_line = int(self.row)

        # раскрвшивание фона для заголовков
        i = 2
        while i <= 13:
            ws[column[i] + "6"].fill = ReportStyle.fill
            i += 1

        # обводка
        # ws['A3'].border = ReportStyle.border

        # вручную устанавливаем высоту первой строки
        rd = ws.row_dimensions[1]
        rd.height = 21.75

        # увеличиваем все строки по высоте
        max_row = ws.max_row
        i = 2
        while i <= max_row:
            rd = ws.row_dimensions[i]
            rd.height = 18
            i += 1

        # Высота строк
        ws.row_dimensions[2].height = 5.25
        ws.row_dimensions[4].height = 6.75
        ws.row_dimensions[end_line].height = 30.75

        # выравнивание столбца
        for cellObj in ws["A2:A5"]:
            for cell in cellObj:
                ws[cell.coordinate].alignment = ReportStyle.align_left

        # перетягивание ячеек
        # https://stackoverflow.com/questions/13197574/openpyxl-adjust-column-width-size
        # dims = {}
        # for row in ws.rows:
        #     for cell in row:
        #         if cell.value:
        #             dims[cell.column] = max((dims.get(cell.column, 0), len(cell.value)))
        # for col, value in dims.items():
        #     # value * коэфициент
        #     ws.column_dimensions[col].width = value * 1.5

        # сохранение файла в текущую директорию
        if itog_report["Дата"][0] == itog_report["Дата"][1] - timedelta(1):
            date_ = datetime.strftime(itog_report["Дата"][0], "%Y-%m-%d")
        else:
            date_ = (
                f'{datetime.strftime(itog_report["Дата"][0], "%Y-%m-%d")} - '
                f'{datetime.strftime(itog_report["Дата"][1] - timedelta(1), "%Y-%m-%d")}'
            )
        path = (
            settings.local_folder
            + settings.report_path
            + date_
            + f' Итоговый отчет по {organisation_total["Организация"]["Организация"][0][0]} '
            + ".xlsx"
        )
        logger.info(
            f"Сохранение Итогового отчета "
            f'по {organisation_total["Организация"]["Организация"][0][0]} в {path}'
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def save_cashdesk_report(self, cashdesk_report, date_from):
        """Сохраняет Суммовой отчет в Excel"""

        column = [
            "",
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M",
            "N",
        ]

        self.row = "0"

        def next_row():
            self.row = str(int(self.row) + 1)
            return self.row

        # объект
        wb = Workbook()

        # активный лист
        ws = wb.active

        # название страницы
        # ws = wb.create_sheet('первая страница', 0)
        ws.title = "Суммовой отчет по чековой ленте"
        # шрифты
        ws["A1"].font = ReportStyle.h1
        # выравнивание
        ws["A1"].alignment = ReportStyle.align_left

        # Ширина стролбцов
        ws.column_dimensions["A"].width = 1 / 7 * 124
        ws.column_dimensions["B"].width = 1 / 7 * 88
        ws.column_dimensions["C"].width = 1 / 7 * 28
        ws.column_dimensions["D"].width = 1 / 7 * 24
        ws.column_dimensions["E"].width = 1 / 7 * 32
        ws.column_dimensions["F"].width = 1 / 7 * 1
        ws.column_dimensions["G"].width = 1 / 7 * 79
        ws.column_dimensions["H"].width = 1 / 7 * 3
        ws.column_dimensions["I"].width = 1 / 7 * 5
        ws.column_dimensions["J"].width = 1 / 7 * 96
        ws.column_dimensions["K"].width = 1 / 7 * 88
        ws.column_dimensions["L"].width = 1 / 7 * 8
        ws.column_dimensions["M"].width = 1 / 7 * 80
        ws.column_dimensions["N"].width = 1 / 7 * 96

        # значение ячейки
        # ws['A1'] = "Hello!"

        ws[column[1] + next_row()] = "Суммовой отчет по чековой ленте"
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        # шрифты
        ws[column[1] + self.row].font = ReportStyle.h1
        # выравнивание
        ws[column[1] + self.row].alignment = ReportStyle.align_left
        # Высота строк
        ws.row_dimensions[1].height = 24

        ws[column[1] + next_row()] = f'{cashdesk_report["Организация"][0][0]}'
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        ws[column[1] + self.row].font = ReportStyle.font
        ws[column[1] + self.row].alignment = ReportStyle.align_top

        ws[column[1] + next_row()] = "За период с:"
        ws[column[1] + self.row].font = ReportStyle.font
        ws[column[1] + self.row].alignment = ReportStyle.align_top
        ws[column[2] + self.row] = cashdesk_report["Дата"][0][0].strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=2, end_row=self.row, end_column=3
        )
        ws[column[2] + self.row].font = ReportStyle.font_bold
        ws[column[2] + self.row].alignment = ReportStyle.align_top
        ws[column[4] + self.row] = "по"
        ws[column[4] + self.row].font = ReportStyle.font
        ws[column[4] + self.row].alignment = ReportStyle.align_top
        ws[column[5] + self.row] = (
            cashdesk_report["Дата"][0][1] - timedelta(1)
        ).strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=5, end_row=self.row, end_column=7
        )
        ws[column[5] + self.row].font = ReportStyle.font_bold
        ws[column[5] + self.row].alignment = ReportStyle.align_top

        # ТАБЛИЦА
        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=6
            )
            ws.merge_cells(
                start_row=self.row, start_column=7, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=11, end_row=self.row, end_column=12
            )
            ws[column[1] + self.row].font = ReportStyle.font
            ws[column[3] + self.row].font = ReportStyle.font
            ws[column[7] + self.row].font = ReportStyle.font
            ws[column[10] + self.row].font = ReportStyle.font
            ws[column[11] + self.row].font = ReportStyle.font
            ws[column[13] + self.row].font = ReportStyle.font
            ws[column[14] + self.row].font = ReportStyle.font
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            ws[column[7] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[11] + self.row].alignment = ReportStyle.align_top
            ws[column[13] + self.row].alignment = ReportStyle.align_top
            ws[column[14] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].number_format = "#,##0.00 ₽"
            ws[column[7] + self.row].number_format = "#,##0.00 ₽"
            ws[column[10] + self.row].number_format = "#,##0.00 ₽"
            ws[column[11] + self.row].number_format = "#,##0.00 ₽"
            ws[column[13] + self.row].number_format = "#,##0.00 ₽"
            ws[column[14] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        def merge_table_h3():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=6
            )
            ws.merge_cells(
                start_row=self.row, start_column=7, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=11, end_row=self.row, end_column=12
            )
            ws[column[1] + self.row].font = ReportStyle.h3
            ws[column[3] + self.row].font = ReportStyle.h3
            ws[column[7] + self.row].font = ReportStyle.h3
            ws[column[10] + self.row].font = ReportStyle.h3
            ws[column[11] + self.row].font = ReportStyle.h3
            ws[column[13] + self.row].font = ReportStyle.h3
            ws[column[14] + self.row].font = ReportStyle.h3
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            ws[column[7] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[11] + self.row].alignment = ReportStyle.align_top
            ws[column[13] + self.row].alignment = ReportStyle.align_top
            ws[column[14] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].number_format = "#,##0.00 ₽"
            ws[column[7] + self.row].number_format = "#,##0.00 ₽"
            ws[column[10] + self.row].number_format = "#,##0.00 ₽"
            ws[column[11] + self.row].number_format = "#,##0.00 ₽"
            ws[column[13] + self.row].number_format = "#,##0.00 ₽"
            ws[column[14] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        def merge_table_red():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=6
            )
            ws.merge_cells(
                start_row=self.row, start_column=7, end_row=self.row, end_column=9
            )
            ws.merge_cells(
                start_row=self.row, start_column=11, end_row=self.row, end_column=12
            )
            ws[column[1] + self.row].font = ReportStyle.font_red
            ws[column[3] + self.row].font = ReportStyle.font_red
            ws[column[7] + self.row].font = ReportStyle.font_red
            ws[column[10] + self.row].font = ReportStyle.font_red
            ws[column[11] + self.row].font = ReportStyle.font_red
            ws[column[13] + self.row].font = ReportStyle.font_red
            ws[column[14] + self.row].font = ReportStyle.font_red
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            ws[column[7] + self.row].alignment = ReportStyle.align_top
            ws[column[10] + self.row].alignment = ReportStyle.align_top
            ws[column[11] + self.row].alignment = ReportStyle.align_top
            ws[column[13] + self.row].alignment = ReportStyle.align_top
            ws[column[14] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].number_format = "#,##0.00 ₽"
            ws[column[7] + self.row].number_format = "#,##0.00 ₽"
            ws[column[10] + self.row].number_format = "#,##0.00 ₽"
            ws[column[11] + self.row].number_format = "#,##0.00 ₽"
            ws[column[13] + self.row].number_format = "#,##0.00 ₽"
            ws[column[14] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        def merge_width_red():
            ws.merge_cells(
                start_row=self.row,
                start_column=1,
                end_row=self.row,
                end_column=len(column) - 1,
            )
            ws[column[1] + self.row].font = ReportStyle.font_red
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            b = 1
            while b < len(column):
                if b == 1:
                    ws[column[b] + self.row].border = ReportStyle.border_left
                elif b == len(column) - 1:
                    ws[column[b] + self.row].border = ReportStyle.border_right
                else:
                    ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        ws[column[1] + next_row()] = "Касса №"
        ws[column[3] + self.row] = "Сумма"
        ws[column[7] + self.row] = "Наличными"
        ws[column[10] + self.row] = "Безналичными"
        ws[column[11] + self.row] = "Со счета"
        ws[column[13] + self.row] = "Бонусами"
        ws[column[14] + self.row] = "MaxBonux"
        merge_table_h3()
        # раскрвшивание фона для заголовков
        b = 1
        while b < len(column):
            ws[column[b] + self.row].fill = ReportStyle.fill
            b += 1

        for typpe in cashdesk_report:
            if typpe != "Дата" and typpe != "Организация":
                if typpe != "Итого":
                    ws[column[1] + next_row()] = typpe
                    merge_width_red()
                for line in cashdesk_report[typpe]:
                    ws[column[1] + next_row()] = line[0]
                    ws[column[3] + self.row] = line[1]
                    ws[column[7] + self.row] = line[2]
                    ws[column[10] + self.row] = line[3]
                    ws[column[11] + self.row] = line[4]
                    ws[column[13] + self.row] = line[5]
                    ws[column[14] + self.row] = line[6] - line[3]
                    if line[0] == "Итого":
                        merge_table_red()
                    elif line[0] == "Итого по отчету":
                        merge_table_h3()
                    else:
                        merge_table()

        # увеличиваем все строки по высоте
        max_row = ws.max_row
        i = 2
        while i <= max_row:
            rd = ws.row_dimensions[i]
            rd.height = 18
            i += 1
        if cashdesk_report["Дата"][0][0] == cashdesk_report["Дата"][0][1] - timedelta(
            1
        ):
            date_ = datetime.strftime(cashdesk_report["Дата"][0][0], "%Y-%m-%d")
        else:
            date_ = (
                f'{datetime.strftime(cashdesk_report["Дата"][0][0], "%Y-%m-%d")} - '
                f'{datetime.strftime(cashdesk_report["Дата"][0][1] - timedelta(1), "%Y-%m-%d")}'
            )
        path = (
            settings.local_folder
            + settings.report_path
            + date_
            + f' Суммовой отчет по {cashdesk_report["Организация"][0][0]}'
            + ".xlsx"
        )
        logger.info(
            f"Сохранение Суммового отчета "
            f'по {cashdesk_report["Организация"][0][0]} в {path}'
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def save_client_count_totals(self, client_count_totals_org, date_from):
        """
        Сохраняет отчет по количеству клиентов за день в Excel
        """

        column = ["", "A", "B", "C", "D", "E"]

        self.row = "0"

        def next_row():
            self.row = str(int(self.row) + 1)
            return self.row

        # объект
        wb = Workbook()

        # активный лист
        ws = wb.active

        # название страницы
        # ws = wb.create_sheet('первая страница', 0)
        ws.title = "Количество человек за день"
        # шрифты
        ws["A1"].font = ReportStyle.h1
        # выравнивание
        ws["A1"].alignment = ReportStyle.align_left

        # Ширина стролбцов
        ws.column_dimensions["A"].width = 1 / 7 * 124
        ws.column_dimensions["B"].width = 1 / 7 * 21
        ws.column_dimensions["C"].width = 1 / 7 * 95
        ws.column_dimensions["D"].width = 1 / 7 * 24
        ws.column_dimensions["E"].width = 1 / 7 * 80

        # значение ячейки
        # ws['A1'] = "Hello!"

        ws[column[1] + next_row()] = "Количество человек за день"
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        # шрифты
        ws[column[1] + self.row].font = ReportStyle.h1
        # выравнивание
        ws[column[1] + self.row].alignment = ReportStyle.align_left
        # Высота строк
        ws.row_dimensions[1].height = 24

        ws[column[1] + next_row()] = f"{client_count_totals_org[0][0]}"
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        ws[column[1] + self.row].font = ReportStyle.font
        ws[column[1] + self.row].alignment = ReportStyle.align_top

        ws[column[1] + next_row()] = "За период с:"
        ws[column[1] + self.row].font = ReportStyle.font
        ws[column[1] + self.row].alignment = ReportStyle.align_top
        ws[column[2] + self.row] = client_count_totals_org[1][0].strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=2, end_row=self.row, end_column=3
        )
        ws[column[2] + self.row].font = ReportStyle.font_bold
        ws[column[2] + self.row].alignment = ReportStyle.align_top
        ws[column[4] + self.row] = "по"
        ws[column[4] + self.row].font = ReportStyle.font
        ws[column[4] + self.row].alignment = ReportStyle.align_top
        ws[column[5] + self.row] = (client_count_totals_org[-2][0]).strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=5, end_row=self.row, end_column=7
        )
        ws[column[5] + self.row].font = ReportStyle.font_bold
        ws[column[5] + self.row].alignment = ReportStyle.align_top

        # ТАБЛИЦА
        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=5
            )
            ws[column[1] + self.row].font = ReportStyle.font
            ws[column[3] + self.row].font = ReportStyle.font
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        def merge_table_bold():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=5
            )
            ws[column[1] + self.row].font = ReportStyle.font_bold
            ws[column[3] + self.row].font = ReportStyle.font_bold
            ws[column[1] + self.row].alignment = ReportStyle.align_top
            ws[column[3] + self.row].alignment = ReportStyle.align_top
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = ReportStyle.border
                b += 1

        ws[column[1] + next_row()] = "Дата"
        ws[column[3] + self.row] = "Количество клиентов"
        merge_table_bold()
        # раскрвшивание фона для заголовков
        b = 1
        while b < len(column):
            ws[column[b] + self.row].fill = ReportStyle.fill
            b += 1

        for line in client_count_totals_org:
            try:
                ws[column[1] + next_row()] = line[0].strftime("%d.%m.%Y")
                ws[column[3] + self.row] = line[1]
                merge_table()
            except AttributeError:
                pass

        ws[column[1] + next_row()] = "Итого"
        ws[column[3] + self.row] = client_count_totals_org[-1][1]
        merge_table_bold()

        # увеличиваем все строки по высоте
        max_row = ws.max_row
        i = 2
        while i <= max_row:
            rd = ws.row_dimensions[i]
            rd.height = 18
            i += 1
        if client_count_totals_org[0][1]:
            date_ = datetime.strftime(client_count_totals_org[1][0], "%Y-%m")
        else:
            date_ = (
                f'{datetime.strftime(client_count_totals_org[1][0], "%Y-%m-%d")} - '
                f'{datetime.strftime(client_count_totals_org[-2][0], "%Y-%m-%d")}'
            )
        path = (
            settings.local_folder
            + settings.report_path
            + date_
            + f" Количество клиентов за день по {client_count_totals_org[0][0]}"
            + ".xlsx"
        )
        logger.info(
            f"Сохранение отчета по количеству клиентов "
            f"по {client_count_totals_org[0][0]} в {path}"
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def create_path(self, path: str, date_from: datetime):
        """Проверяет наличие указанного пути. В случае отсутствия каких-либо папок создает их."""

        logger.info("Проверка локальных путей сохранения файлов...")
        list_path = path.split("/")
        path = ""
        end_path = ""
        if list_path[-1][-4:] == ".xls" or list_path[-1]:
            end_path = list_path.pop()
        list_path.append(date_from.strftime("%Y"))
        list_path.append(date_from.strftime("%m") + "-" + date_from.strftime("%B"))
        directory = os.getcwd()
        for folder in list_path:
            if folder not in os.listdir():
                os.mkdir(folder)
                logger.debug(f'В директории "{os.getcwd()}" создана папка "{folder}"')
                os.chdir(folder)
            else:
                os.chdir(folder)
            path += folder + "/"
        path += end_path
        os.chdir(directory)
        return path

    def save_file(self, path, file):
        """Проверяет не занят ли файл другим процессом и если нет, то перезаписывает его, в противном
        случае выводит диалоговое окно с предложением закрыть файл и продолжить.
        """
        try:
            file.save(path)
        except PermissionError as e:
            logger.error(f'Файл "{path}" занят другим процессом.\n{repr(e)}')

    def create_path_yadisk(self, path, date_from, yadisk):
        """Проверяет наличие указанного пути в Яндекс Диске.

        В случае отсутствия каких-либо папок создает их.
        """
        logger.info("Проверка путей сохранения файлов на Яндекс.Диске...")
        list_path = path.split("/")
        if list_path[-1][-4:] == ".xls" or list_path[-1] == "":
            list_path.pop()
        list_path.append(date_from.strftime("%Y"))
        list_path.append(date_from.strftime("%m") + "-" + date_from.strftime("%B"))
        directory = "/"
        list_path_yandex = []
        for folder in list_path:
            folder = directory + folder
            directory = folder + "/"
            list_path_yandex.append(folder)
        directory = "/"
        for folder in list_path_yandex:
            folders_list = []
            folders_list_yandex = list(yadisk.listdir(directory))
            for key in folders_list_yandex:
                if not key["file"]:
                    folders_list.append(directory + key["name"])

            if folder not in folders_list:
                yadisk.mkdir(folder)
                logger.info(f'Создание новой папки в YandexDisk - "{folder}"')
                directory = folder + "/"
            else:
                directory = folder + "/"
        path = list_path_yandex[-1] + "/"
        return path

    def sync_to_yadisk(
        self, path_list: list[str], token: str, date_from: datetime
    ) -> list[SyncResourceLinkObject]:
        """Копирует локальные файлы в Яндекс Диск."""

        logger.info("Копирование отчетов в Яндекс.Диск...")
        if not path_list:
            logger.warning("Нет ни одного отчета для отправки в Yandex.Disk")
            return None

        links = []
        yadisk = YaDisk(token=token)
        if yadisk.check_token():
            path = "" + settings.report_path
            remote_folder = self.create_path_yadisk(path, date_from, yadisk)
            for local_path in path_list:
                remote_path = remote_folder + local_path.split("/")[-1]
                file_name = f"'{local_path.split('/')[-1]}'"
                files_list_yandex = list(yadisk.listdir(remote_folder))
                files_list = []
                for key in files_list_yandex:
                    if key["file"]:
                        files_list.append(remote_folder + key["name"])
                if remote_path in files_list:
                    logger.warning(
                        f"Файл {file_name} уже существует в '{remote_folder}' и будет заменен!"
                    )
                    yadisk.remove(remote_path, permanently=True)
                links.append(yadisk.upload(local_path, remote_path))
            return links
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Ошибка YaDisk: token не валиден",
            )


def get_yandex_repo() -> YandexRepository:
    yandex_repo = YandexRepository()
    return yandex_repo
