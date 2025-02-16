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


class YandexRepository:
    def __init__(self):
        pass

    def save_corp_services_sum_report(
        self, report, date_from, date_to, hide_zero=False
    ):
        """
        Сохраняет отчет по количеству клиентов за день в Excel
        """
        # определяем стили
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
        align_left = Alignment(
            horizontal="left",
            vertical="bottom",
            text_rotation=0,
            wrap_text=False,
            shrink_to_fit=False,
            indent=0,
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
        align_left = Alignment(
            horizontal="left",
            vertical="bottom",
            text_rotation=0,
            wrap_text=False,
            shrink_to_fit=False,
            indent=0,
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
        ws.title = "Суммы трат в аквазоне"
        # шрифты
        ws["C1"].font = h1
        # выравнивание
        ws["C1"].alignment = align_left

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

        ws[column[3] + next_row()] = "Суммы трат в аквазоне по КОРП билетам"
        ws.merge_cells(
            start_row=self.row, start_column=3, end_row=self.row, end_column=12
        )
        ws[column[1] + next_row()] = ""
        ws[column[3] + next_row()] = ""
        ws.merge_cells(
            start_row=self.row, start_column=3, end_row=self.row, end_column=12
        )
        ws[column[3] + self.row].font = font
        ws[column[3] + self.row].alignment = align_top
        ws[column[1] + next_row()] = ""

        if date_from == date_to - timedelta(days=1):
            ws[column[3] + next_row()] = "За:"
            ws[column[3] + self.row].font = font
            ws[column[3] + self.row].alignment = align_top
            ws[column[5] + self.row] = date_from.strftime("%d.%m.%Y")
            ws[column[5] + self.row].font = font_bold
            ws[column[5] + self.row].alignment = align_top
        else:
            ws[column[3] + next_row()] = "За период с:"
            ws[column[3] + self.row].font = font
            ws[column[3] + self.row].alignment = align_top
            ws[column[5] + self.row] = date_from.strftime("%d.%m.%Y")
            ws[column[5] + self.row].font = font_bold
            ws[column[5] + self.row].alignment = align_top
            ws[column[7] + self.row] = "По:"
            ws[column[7] + self.row].font = font
            ws[column[7] + self.row].alignment = align_top
            ws[column[9] + self.row] = (date_to - timedelta(days=1)).strftime(
                "%d.%m.%Y"
            )
            ws[column[9] + self.row].font = font_bold
            ws[column[9] + self.row].alignment = align_top

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
            ws[column[2] + self.row].font = font
            ws[column[10] + self.row].font = font
            ws[column[12] + self.row].font = font
            ws[column[2] + self.row].alignment = align_top
            ws[column[10] + self.row].alignment = align_top
            ws[column[12] + self.row].alignment = align_top
            b = 2
            while b <= 13:
                ws[column[b] + self.row].border = border
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
            ws[column[2] + self.row].font = h3
            ws[column[10] + self.row].font = h3
            ws[column[12] + self.row].font = h3
            ws[column[2] + self.row].alignment = align_top
            ws[column[10] + self.row].alignment = align_top
            ws[column[12] + self.row].alignment = align_top
            ws[column[2] + self.row].border = border_left
            ws[column[13] + self.row].border = border_right

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
            ws[column[2] + self.row].font = h2
            ws[column[10] + self.row].font = h2
            ws[column[12] + self.row].font = h2
            ws[column[2] + self.row].alignment = align_top
            ws[column[10] + self.row].alignment = align_top
            ws[column[12] + self.row].alignment = align_top
            ws[column[2] + self.row].border = border_left
            ws[column[13] + self.row].border = border_right

        def merge_width_h2():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = h2
            ws[column[2] + self.row].alignment = align_top
            b = 2
            while b <= 13:
                if b == 2:
                    ws[column[b] + self.row].border = border_left_top
                elif b == 13:
                    ws[column[b] + self.row].border = border_right_top
                else:
                    ws[column[b] + self.row].border = border_top
                b += 1

        def merge_width_h3():
            ws.merge_cells(
                start_row=self.row, start_column=2, end_row=self.row, end_column=13
            )
            ws[column[2] + self.row].font = h3
            ws[column[2] + self.row].alignment = align_top
            b = 2
            while b <= 13:
                if b == 2:
                    ws[column[b] + self.row].border = border_left
                elif b == 13:
                    ws[column[b] + self.row].border = border_right
                b += 1

        ws[column[2] + next_row()] = "Наименование услуги"
        ws[column[10] + self.row] = "Количество"
        ws[column[12] + self.row] = "Сумма"
        merge_table()
        ws[column[2] + self.row].font = h3
        ws[column[10] + self.row].font = h3
        ws[column[12] + self.row].font = h3
        ws[column[2] + self.row].alignment = align_top
        ws[column[10] + self.row].alignment = align_top
        ws[column[12] + self.row].alignment = align_top

        for line in report:
            if hide_zero and line.summ == Decimal(0):
                continue

            ws[column[2] + next_row()] = (
                line.name[7:] if line.name.startswith("Долг за") else line.name
            )
            ws[column[10] + self.row] = line.count
            ws[column[12] + self.row] = line.summ
            ws[column[12] + self.row].number_format = "#,##0.00 ₽"
            merge_table()

        ws[column[2] + next_row()] = "Итого"
        ws[column[10] + self.row] = sum(line.count for line in report)
        ws[column[12] + self.row] = sum(line.summ for line in report)
        ws[column[12] + self.row].number_format = "#,##0.00 ₽"
        merge_table_h2()
        ws[column[2] + self.row].alignment = align_bottom
        ws[column[10] + self.row].alignment = align_bottom
        ws[column[12] + self.row].alignment = align_bottom
        b = 2
        while b <= 13:
            ws[column[b] + self.row].border = border_top_bottom
            b += 1
        end_line = int(self.row)

        # раскрвшивание фона для заголовков
        i = 2
        while i <= 13:
            ws[column[i] + "6"].fill = fill
            i += 1

        # обводка
        # ws['A3'].border = border

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
                ws[cell.coordinate].alignment = align_left

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
            + " Суммы трат в аквазоне по КОРП тарифам.xlsx"
        )
        logger.info(
            f"Сохранение отчета по сумме трат в аквазоне по КОРП тарифам за {date_} в {path}"
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def create_path(self, path, date_from):
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
