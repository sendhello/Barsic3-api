import logging
from datetime import datetime, timedelta

import apiclient
from dateutil.relativedelta import relativedelta

from core.settings import settings
from legacy import functions


logger = logging.getLogger(__name__)


class SpreadsheetError(Exception):
    pass


class SpreadsheetNotSetError(SpreadsheetError):
    pass


class SheetNotSetError(SpreadsheetError):
    pass


def create_columnDict() -> dict:
    columnDict = {}
    for ch1 in range(ord("A") - 1, ord("C") + 1):
        for ch2 in range(ord("A"), ord("Z") + 1):
            if ch1 == ord("A") - 1:
                columnDict[chr(ch2)] = ch2 - ord("A")
            else:
                columnDict[f"{chr(ch1)}{chr(ch2)}"] = (
                    26 * (ch1 - ord("A") + 1) + ch2 - ord("A")
                )
    return columnDict


def get_letter_column_name(column: int) -> str:
    first_letter = chr(column // 26 + ord("@"))
    second_letter = chr(column % 26 + ord("@"))
    if first_letter == "@":
        return second_letter

    return f"{first_letter}{second_letter}"


class Spreadsheet:

    # Класс-оберта методов
    def __init__(self, spreadsheetId, sheetId, service, sheetTitle):
        self.requests = []
        self.valueRanges = []
        self.spreadsheetId = spreadsheetId
        self.sheetId = sheetId
        self.service = service
        self.sheetTitle = sheetTitle

    def prepare_setDimensionPixelSize(self, dimension, startIndex, endIndex, pixelSize):
        self.requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": self.sheetId,
                        "dimension": dimension,
                        "startIndex": startIndex,
                        "endIndex": endIndex,
                    },
                    "properties": {"pixelSize": pixelSize},
                    "fields": "pixelSize",
                }
            }
        )

    def prepare_setColumnsWidth(self, startCol, endCol, width):
        self.prepare_setDimensionPixelSize("COLUMNS", startCol, endCol + 1, width)

    def prepare_setColumnWidth(self, col, width):
        self.prepare_setColumnsWidth(col, col, width)

    def prepare_setValues(self, cellsRange, values, majorDimension="ROWS"):
        self.valueRanges.append(
            {
                "range": self.sheetTitle + "!" + cellsRange,
                "majorDimension": majorDimension,
                "values": values,
            }
        )

    # spreadsheets.batchUpdate and spreadsheets.values.batchUpdate
    def runPrepared(self, valueInputOption="USER_ENTERED"):
        upd1Res = {"replies": []}
        upd2Res = {"responses": []}
        try:
            if len(self.requests) > 0:
                upd1Res = (
                    self.service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=self.spreadsheetId,
                        body={"requests": self.requests},
                    )
                    .execute()
                )
            if len(self.valueRanges) > 0:
                upd2Res = (
                    self.service.spreadsheets()
                    .values()
                    .batchUpdate(
                        spreadsheetId=self.spreadsheetId,
                        body={
                            "valueInputOption": valueInputOption,
                            "data": self.valueRanges,
                        },
                    )
                    .execute()
                )
        finally:
            self.requests = []
            self.valueRanges = []
        return (upd1Res["replies"], upd2Res["responses"])

        # Converts string range to GridRange of current sheet; examples:
        #   "A3:B4" -> {sheetId: id of current sheet, startRowIndex: 2,
        #   endRowIndex: 4, startColumnIndex: 0, endColumnIndex: 2}
        #   "A5:B"  -> {sheetId: id of current sheet, startRowIndex: 4,
        #   startColumnIndex: 0, endColumnIndex: 2}

    def toGridRange(self, cellsRange):
        columnDict = create_columnDict()
        if self.sheetId is None:
            raise SheetNotSetError()
        if isinstance(cellsRange, str):
            startCell, endCell = cellsRange.split(":")[0:2]
            cellsRange = {}
            i = 0
            for s in startCell:
                if s.isdigit():
                    startCellColumn = startCell[:i]
                    startCellRow = int(startCell[i:])
                    break
                i += 1
            i = 0
            for s in endCell:
                if s.isdigit():
                    endCellColumn = endCell[:i]
                    endCellRow = int(endCell[i:])
                    break
                i += 1
            try:
                cellsRange["startColumnIndex"] = int(columnDict[startCellColumn])
                cellsRange["endColumnIndex"] = int(columnDict[endCellColumn]) + 1
            except KeyError:
                raise (
                    KeyError,
                    'Possible, Key Columns out range. Please added it in method "create_columnDict".',
                )
            if startCellRow > 0:
                cellsRange["startRowIndex"] = startCellRow - 1
            if endCellRow > 0:
                cellsRange["endRowIndex"] = endCellRow
        cellsRange["sheetId"] = self.sheetId
        return cellsRange

    def prepare_mergeCells(self, cellsRange, mergeType="MERGE_ALL"):
        self.requests.append(
            {
                "mergeCells": {
                    "range": self.toGridRange(cellsRange),
                    "mergeType": mergeType,
                }
            }
        )

    # formatJSON should be dict with userEnteredFormat to be applied to each cell
    def prepare_setCellsFormat(
        self, cellsRange, formatJSON, fields="userEnteredFormat"
    ):
        self.requests.append(
            {
                "repeatCell": {
                    "range": self.toGridRange(cellsRange),
                    "cell": {"userEnteredFormat": formatJSON},
                    "fields": fields,
                }
            }
        )

    # formatsJSON should be list of lists of dicts with userEnteredFormat for each cell in each row
    def prepare_setCellsFormats(
        self, cellsRange, formatsJSON, fields="userEnteredFormat"
    ):
        self.requests.append(
            {
                "updateCells": {
                    "range": self.toGridRange(cellsRange),
                    "rows": [
                        {
                            "values": [
                                {"userEnteredFormat": cellFormat}
                                for cellFormat in rowFormats
                            ]
                        }
                        for rowFormats in formatsJSON
                    ],
                    "fields": fields,
                }
            }
        )

    def prepare_setBorderFormats(self, cellsRange, fields="userEnteredFormat"):
        self.requests.append(
            {
                "updateBorders": {
                    "range": self.toGridRange(cellsRange),
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                    "top": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                    "left": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                    "right": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                }
            }
        )


def create_new_google_doc(
    googleservice,
    doc_name: str,
    data_report,
    finreport_dict,
    http_auth,
    date_from,
    sheet_width,
    sheet2_width,
    sheet3_width,
    sheet4_width,
    sheet5_width,
    sheet6_width,
    sheet_height,
    sheet2_height,
    sheet4_height,
    sheet5_height,
    sheet6_height,
) -> tuple[str, str]:
    """Создание нового google-документа."""

    logging.info("Создание Google-документа...")
    spreadsheet = (
        googleservice.spreadsheets()
        .create(
            body={
                "properties": {"title": doc_name, "locale": "ru_RU"},
                "sheets": [
                    {
                        "properties": {
                            "sheetType": "GRID",
                            "sheetId": 0,
                            "title": "Сводный",
                            "gridProperties": {
                                "rowCount": sheet_height,
                                "columnCount": sheet_width,
                            },
                        }
                    },
                    {
                        "properties": {
                            "sheetType": "GRID",
                            "sheetId": 1,
                            "title": "Смайл",
                            "gridProperties": {
                                "rowCount": sheet2_height,
                                "columnCount": sheet2_width,
                            },
                        }
                    },
                    {
                        "properties": {
                            "sheetType": "GRID",
                            "sheetId": 2,
                            "title": "План",
                            "gridProperties": {
                                "rowCount": sheet_height,
                                "columnCount": sheet3_width,
                            },
                        }
                    },
                    {
                        "properties": {
                            "sheetType": "GRID",
                            "sheetId": 3,
                            "title": "Итоговый",
                            "gridProperties": {
                                "rowCount": sheet4_height,
                                "columnCount": sheet4_width,
                            },
                        }
                    },
                    {
                        "properties": {
                            "sheetType": "GRID",
                            "sheetId": 4,
                            "title": "Итоговый ПА",
                            "gridProperties": {
                                "rowCount": sheet5_height,
                                "columnCount": sheet5_width,
                            },
                        }
                    },
                    {
                        "properties": {
                            "sheetType": "GRID",
                            "sheetId": 5,
                            "title": "Пляж",
                            "gridProperties": {
                                "rowCount": sheet6_height,
                                "columnCount": sheet6_width,
                            },
                        }
                    },
                ],
            }
        )
        .execute()
    )

    # Доступы к документу
    logging.info("Настройка доступов к файлу GoogleSheets...")
    driveService = apiclient.discovery.build(
        "drive", "v3", http=http_auth, cache_discovery=False
    )
    if settings.google_api_settings.google_all_read:
        _ = (
            driveService.permissions()
            .create(
                fileId=spreadsheet["spreadsheetId"],
                body={
                    "type": "anyone",
                    "role": "reader",
                },  # доступ на чтение кому угодно
                fields="id",
            )
            .execute()
        )
    # Возможные значения writer, commenter, reader
    # доступ на Чтение определенным пользователоям
    google_reader_list = [
        address
        for address in settings.google_api_settings.google_reader_list.split(",")
        if address
    ]
    for address in google_reader_list:
        _ = (
            driveService.permissions()
            .create(
                fileId=spreadsheet["spreadsheetId"],
                body={
                    "type": "user",
                    "role": "reader",
                    "emailAddress": address,
                },
                fields="id",
            )
            .execute()
        )
    # доступ на Запись определенным пользователоям
    google_writer_list = [
        address
        for address in settings.google_api_settings.google_writer_list.split(",")
        if address
    ]
    for address in google_writer_list:
        _ = (
            driveService.permissions()
            .create(
                fileId=spreadsheet["spreadsheetId"],
                body={
                    "type": "user",
                    "role": "writer",
                    "emailAddress": address,
                },
                fields="id",
            )
            .execute()
        )

    # ЛИСТ 1
    logging.info("Создание листа 1 в файле GoogleSheets...")
    sheetId = 0
    # Ширина столбцов
    ss = Spreadsheet(
        spreadsheet["spreadsheetId"],
        sheetId,
        googleservice,
        spreadsheet["sheets"][sheetId]["properties"]["title"],
    )
    # Дата, День недели
    ss.prepare_setColumnsWidth(0, 1, 105)
    # Кол-во проходов ПЛАН - Общая сумма {month} {year}
    ss.prepare_setColumnsWidth(2, 9, 120)
    # Билеты
    ss.prepare_setColumnWidth(10, 65)
    ss.prepare_setColumnWidth(11, 120)
    ss.prepare_setColumnWidth(12, 100)
    # Депозит, Штраф
    ss.prepare_setColumnsWidth(13, 14, 100)
    # Общепит ПЛАН
    ss.prepare_setColumnWidth(15, 65)
    ss.prepare_setColumnWidth(16, 120)
    ss.prepare_setColumnWidth(17, 100)
    # Общепит ФАКТ
    ss.prepare_setColumnWidth(18, 65)
    ss.prepare_setColumnWidth(19, 120)
    ss.prepare_setColumnWidth(20, 100)
    # Общепит LASTYEAR
    ss.prepare_setColumnWidth(21, 65)
    ss.prepare_setColumnWidth(22, 120)
    ss.prepare_setColumnWidth(23, 100)
    # Фотоуслуги ПЛАН
    ss.prepare_setColumnWidth(24, 65)
    ss.prepare_setColumnWidth(25, 120)
    ss.prepare_setColumnWidth(26, 100)
    # Фотоуслуги ФАКТ
    ss.prepare_setColumnWidth(27, 65)
    ss.prepare_setColumnWidth(28, 120)
    ss.prepare_setColumnWidth(29, 100)
    # Фотоуслуги LASTYEAR
    ss.prepare_setColumnWidth(30, 65)
    ss.prepare_setColumnWidth(31, 120)
    ss.prepare_setColumnWidth(32, 100)
    # УЛËТSHOP ПЛАН
    ss.prepare_setColumnWidth(33, 65)
    ss.prepare_setColumnWidth(34, 120)
    ss.prepare_setColumnWidth(35, 100)
    # УЛËТSHOP ФАКТ
    ss.prepare_setColumnWidth(36, 65)
    ss.prepare_setColumnWidth(37, 120)
    ss.prepare_setColumnWidth(38, 100)
    # УЛËТSHOP LASTYEAR
    ss.prepare_setColumnWidth(39, 65)
    ss.prepare_setColumnWidth(40, 120)
    ss.prepare_setColumnWidth(41, 100)
    # Аренда полотенец ПЛАН
    ss.prepare_setColumnWidth(42, 65)
    ss.prepare_setColumnWidth(43, 120)
    ss.prepare_setColumnWidth(44, 100)
    # Аренда полотенец ФАКТ
    ss.prepare_setColumnWidth(45, 65)
    ss.prepare_setColumnWidth(46, 120)
    ss.prepare_setColumnWidth(47, 100)
    # Аренда полотенец LASTYEAR
    ss.prepare_setColumnWidth(48, 65)
    ss.prepare_setColumnWidth(49, 120)
    ss.prepare_setColumnWidth(50, 100)
    # Фишпиллинг ПЛАН
    ss.prepare_setColumnWidth(51, 65)
    ss.prepare_setColumnWidth(52, 120)
    ss.prepare_setColumnWidth(53, 100)
    # Фишпиллинг ФАКТ
    ss.prepare_setColumnWidth(54, 65)
    ss.prepare_setColumnWidth(55, 120)
    ss.prepare_setColumnWidth(56, 100)
    # Фишпиллинг LASTYEAR
    ss.prepare_setColumnWidth(57, 65)
    ss.prepare_setColumnWidth(58, 120)
    ss.prepare_setColumnWidth(59, 100)
    # Билеты КОРП
    ss.prepare_setColumnWidth(60, 65)
    ss.prepare_setColumnWidth(61, 120)
    ss.prepare_setColumnWidth(62, 100)
    # Прочее
    ss.prepare_setColumnWidth(63, 65)
    ss.prepare_setColumnWidth(64, 120)
    # Online Продажи
    ss.prepare_setColumnWidth(64, 65)
    ss.prepare_setColumnWidth(66, 120)
    ss.prepare_setColumnWidth(67, 100)
    # Нулевые
    ss.prepare_setColumnWidth(68, 65)
    ss.prepare_setColumnWidth(69, 120)
    ss.prepare_setColumnWidth(70, 100)
    # Сумма безнал
    ss.prepare_setColumnWidth(71, 120)
    # Онлайн прочее
    ss.prepare_setColumnWidth(72, 120)

    # Объединение ячеек

    # Дата
    ss.prepare_mergeCells("A1:A2")
    # День недели
    ss.prepare_mergeCells("B1:B2")
    # Кол-во проходов ПЛАН
    ss.prepare_mergeCells("C1:C2")
    # Кол-во проходов ФАКТ
    ss.prepare_mergeCells("D1:D2")
    # Кол-во проходов LASTYEAR
    ss.prepare_mergeCells("E1:E2")
    # Общая сумма ПЛАН
    ss.prepare_mergeCells("F1:F2")
    # Общая сумма ФАКТ
    ss.prepare_mergeCells("G1:G2")
    # Средний чек ФАКТ
    ss.prepare_mergeCells("H1:H2")
    # Бонусы
    ss.prepare_mergeCells("I1:I2")
    # Общая сумма LASTYEAR
    ss.prepare_mergeCells("J1:J2")
    # Билеты
    ss.prepare_mergeCells("K1:M1")
    # Депозит
    ss.prepare_mergeCells("N1:N2")
    # Штраф
    ss.prepare_mergeCells("O1:O2")
    # Общепит ПЛАН
    ss.prepare_mergeCells("P1:R1")
    # Общепит ФАКТ
    ss.prepare_mergeCells("S1:U1")
    # Общепит LASTYEAR
    ss.prepare_mergeCells("V1:X1")
    # Фотоуслуги ПЛАН
    ss.prepare_mergeCells("Y1:AA1")
    # Фотоуслуги ФАКТ
    ss.prepare_mergeCells("AB1:AD1")
    # Фотоуслуги LASTYEAR
    ss.prepare_mergeCells("AE1:AG1")
    # УЛËТSHOP ПЛАН
    ss.prepare_mergeCells("AH1:AJ1")
    # УЛËТSHOP ФАКТ
    ss.prepare_mergeCells("AK1:AM1")
    # УЛËТSHOP LASTYEAR
    ss.prepare_mergeCells("AN1:AP1")
    # Аренда полотенец ПЛАН
    ss.prepare_mergeCells("AQ1:AS1")
    # Аренда полотенец ФАКТ
    ss.prepare_mergeCells("AT1:AV1")
    # Аренда полотенец LASTYEAR
    ss.prepare_mergeCells("AW1:AY1")
    # Фишпиллинг ПЛАН
    ss.prepare_mergeCells("AZ1:BB1")
    # Фишпиллинг ФАКТ
    ss.prepare_mergeCells("BC1:BE1")
    # Фишпиллинг LASTYEAR
    ss.prepare_mergeCells("BF1:BH1")
    # Билеты КОРП
    ss.prepare_mergeCells("BI1:BK1")
    # Прочее
    ss.prepare_mergeCells("BL1:BM1")
    # Online Продажи
    ss.prepare_mergeCells("BN1:BP1")
    # Нулевые
    ss.prepare_mergeCells("BQ1:BS1")
    # Сумма безнал
    ss.prepare_mergeCells("BT1:BT2")
    # Онлайн прочее
    ss.prepare_mergeCells("BU1:BU2")

    # Задание параметров группе ячеек
    # Жирный, по центру
    ss.prepare_setCellsFormat(
        "A1:BU2",
        {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
    )
    # ss.prepare_setCellsFormat('E4:E8', {'numberFormat': {'pattern': '[h]:mm:ss', 'type': 'TIME'}},
    #                           fields='userEnteredFormat.numberFormat')

    # Заполнение таблицы
    ss.prepare_setValues(
        "A1:BU2",
        [
            [
                "Дата",
                "День недели",
                "Кол-во проходов \nПЛАН",
                "Кол-во проходов \nФАКТ",
                f"Кол-во проходов \n{data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "Общая сумма \nПЛАН",
                "Общая сумма \nФАКТ",
                "Средний чек \nФАКТ",
                "Бонусы",
                f"Общая сумма \n{data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "Билеты",
                "",
                "",
                "Депозит",
                "Штраф",
                "Общепит ПЛАН",
                "",
                "",
                "Общепит ФАКТ",
                "",
                "",
                f"Общепит {data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "",
                "",
                "Фотоуслуги ПЛАН",
                "",
                "",
                "Фотоуслуги ФАКТ",
                "",
                "",
                f"Фотоуслуги {data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "",
                "",
                "УЛËТSHOP ПЛАН",
                "",
                "",
                "УЛËТSHOP ФАКТ",
                "",
                "",
                f"УЛËТSHOP {data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "",
                "",
                "Аренда полотенец ПЛАН",
                "",
                "",
                "Аренда полотенец ФАКТ",
                "",
                "",
                f"Аренда полотенец {data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "",
                "",
                "Фишпиллинг ПЛАН",
                "",
                "",
                "Фишпиллинг ФАКТ",
                "",
                "",
                f"Фишпиллинг {data_report} "
                f"{datetime.strftime(finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                "",
                "",
                "Билеты КОРП",
                "",
                "",
                "Прочее",
                "",
                "Online Продажи",
                "",
                "",
                "Нулевые",
                "",
                "",
                "Сумма безнал",
                "Онлайн прочее",
            ],
            [
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "",
                "",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "",
                "",
            ],
        ],
        "ROWS",
    )
    # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        "A1:BU2",
        {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    for i in range(2):
        for j in range(sheet_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "top": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )

    ss.runPrepared()

    # ЛИСТ 2
    logging.info(
        f"{__name__}: {str(datetime.now())[:-7]}:    "
        f"Создание листа 2 в файле GoogleSheets..."
    )
    sheetId = 1
    # Ширина столбцов
    ss = Spreadsheet(
        spreadsheet["spreadsheetId"],
        sheetId,
        googleservice,
        spreadsheet["sheets"][sheetId]["properties"]["title"],
    )
    ss.prepare_setColumnsWidth(0, 2, 105)

    # Объединение ячеек
    ss.prepare_mergeCells("A1:C1")

    # Задание параметров группе ячеек
    # Жирный, по центру
    ss.prepare_setCellsFormat(
        "A1:C2",
        {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
    )
    # ss.prepare_setCellsFormat('E4:E8', {'numberFormat': {'pattern': '[h]:mm:ss', 'type': 'TIME'}},
    #                           fields='userEnteredFormat.numberFormat')

    # Заполнение таблицы
    ss.prepare_setValues(
        "A1:C2", [["Смайл", "", ""], ["Дата", "Кол-во", "Сумма"]], "ROWS"
    )
    # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        "A1:C2",
        {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    for i in range(2):
        for j in range(sheet2_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "top": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )

    ss.runPrepared()

    # ЛИСТ 3
    logging.info(
        f"{__name__}: {str(datetime.now())[:-7]}:    "
        f"Создание листа 3 в файле GoogleSheets..."
    )
    sheetId = 2
    # Ширина столбцов
    ss = Spreadsheet(
        spreadsheet["spreadsheetId"],
        sheetId,
        googleservice,
        spreadsheet["sheets"][sheetId]["properties"]["title"],
    )
    # Дата, День недели
    ss.prepare_setColumnsWidth(0, 1, 100)
    # Кол-во проходов ПРОГНОЗ - Общая сумма {month} {year}
    ss.prepare_setColumnsWidth(2, 7, 120)
    # Общепит ПЛАН
    ss.prepare_setColumnWidth(8, 65)
    ss.prepare_setColumnWidth(9, 120)
    ss.prepare_setColumnWidth(10, 100)
    # Фотоуслуги ПЛАН
    ss.prepare_setColumnWidth(11, 65)
    ss.prepare_setColumnWidth(12, 120)
    ss.prepare_setColumnWidth(13, 100)
    # УЛËТSHOP ПЛАН
    ss.prepare_setColumnWidth(14, 65)
    ss.prepare_setColumnWidth(15, 120)
    ss.prepare_setColumnWidth(16, 100)
    # Aренда полотенец ПЛАН
    ss.prepare_setColumnWidth(14, 65)
    ss.prepare_setColumnWidth(15, 120)
    ss.prepare_setColumnWidth(16, 100)
    # Фишпиллинг ПЛАН
    ss.prepare_setColumnWidth(14, 65)
    ss.prepare_setColumnWidth(15, 120)
    ss.prepare_setColumnWidth(16, 100)
    # Пляж ПЛАН
    ss.prepare_setColumnWidth(17, 65)
    ss.prepare_setColumnWidth(18, 120)
    ss.prepare_setColumnWidth(19, 100)

    # Объединение ячеек

    # Дата
    ss.prepare_mergeCells("A1:A2")
    # День недели
    ss.prepare_mergeCells("B1:B2")
    # Кол-во проходов ПРОГНОЗ
    ss.prepare_mergeCells("C1:C2")
    # Кол-во проходов ФАКТ
    ss.prepare_mergeCells("D1:D2")
    # Общая сумма ПРОГНОЗ
    ss.prepare_mergeCells("E1:E2")
    # Общая сумма ФАКТ
    ss.prepare_mergeCells("F1:F2")
    # Средний чек ПРОГНОЗ
    ss.prepare_mergeCells("G1:G2")
    # Средний чек ФАКТ
    ss.prepare_mergeCells("H1:H2")
    # Общепит ПЛАН
    ss.prepare_mergeCells("I1:K1")
    # Фотоуслуги ПЛАН
    ss.prepare_mergeCells("L1:N1")
    # УЛËТSHOP ПЛАН
    ss.prepare_mergeCells("O1:Q1")
    # Аренда полотенец ПЛАН
    ss.prepare_mergeCells("R1:T1")
    # Фишпиллинг ПЛАН
    ss.prepare_mergeCells("U1:W1")
    # Пляж ПЛАН
    ss.prepare_mergeCells("X1:Z1")

    # Задание параметров группе ячеек
    # Жирный, по центру
    ss.prepare_setCellsFormat(
        "A1:Z2",
        {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
    )
    # ss.prepare_setCellsFormat('E4:E8', {'numberFormat': {'pattern': '[h]:mm:ss', 'type': 'TIME'}},
    #                           fields='userEnteredFormat.numberFormat')

    # Заполнение таблицы
    ss.prepare_setValues(
        "A1:Z2",
        [
            [
                "Дата",
                "День недели",
                "Кол-во проходов \nПРОГНОЗ",
                "Кол-во проходов \nФАКТ",
                "Общая сумма \nПРОГНОЗ",
                "Общая сумма \nФАКТ",
                "Средний чек \nПРОГНОЗ",
                "Средний чек \nФАКТ",
                "Общепит ПЛАН",
                "",
                "",
                "Фотоуслуги ПЛАН",
                "",
                "",
                "УЛËТSHOP ПЛАН",
                "",
                "",
                "Аренда полотенец ПЛАН",
                "",
                "",
                "Фишпиллинг ПЛАН",
                "",
                "",
                "Пляж ПЛАН",
                "",
                "",
            ],
            [
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Трафик",
                "Общая сумма",
                "Средний чек",
            ],
        ],
        "ROWS",
    )
    # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        "A1:Z2",
        {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    for i in range(2):
        for j in range(sheet3_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "top": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
    # ss.runPrepared()

    # Заполнение таблицы 2
    logging.info(
        f"{__name__}: {str(datetime.now())[:-7]}:    "
        f"Заполнение листа 2 в файле GoogleSheets..."
    )

    # Заполнение строки с данными
    weekday_rus = [
        "Понедельник",
        "Вторник",
        "Среда",
        "Четверг",
        "Пятница",
        "Суббота",
        "Воскресенье",
    ]

    start_date = datetime.strptime(
        f"01{finreport_dict['Дата'][0].strftime('%m%Y')}", "%d%m%Y"
    )
    enddate = start_date + relativedelta(months=1)
    dateline = start_date
    sheet2_line = 3
    while dateline < enddate:
        ss.prepare_setValues(
            f"A{sheet2_line}:Z{sheet2_line}",
            [
                [
                    datetime.strftime(dateline, "%d.%m.%Y"),
                    weekday_rus[dateline.weekday()],
                    "",
                    f"=IF(OR('Сводный'!A{sheet2_line} = \"ИТОГО\";"
                    f"LEFT('Сводный'!A{sheet2_line}; 10) = \"Выполнение\");\"\";'Сводный'!D{sheet2_line})",
                    "",
                    f"=IF(OR('Сводный'!A{sheet2_line} = \"ИТОГО\";"
                    f"LEFT('Сводный'!A{sheet2_line}; 10) = \"Выполнение\");\"\";'Сводный'!G{sheet2_line})",
                    f"=IFERROR(E{sheet2_line}/C{sheet2_line};0)",
                    f"=IFERROR(F{sheet2_line}/D{sheet2_line};0)",
                    "",
                    "",
                    f"=IFERROR(J{sheet2_line}/I{sheet2_line};0)",
                    "",
                    "",
                    f"=IFERROR(M{sheet2_line}/L{sheet2_line};0)",
                    "",
                    "",
                    f"=IFERROR(P{sheet2_line}/O{sheet2_line};0)",
                    "",
                    "",
                    f"=IFERROR(S{sheet2_line}/R{sheet2_line};0)",
                    "",
                    "",
                    f"=IFERROR(V{sheet2_line}/U{sheet2_line};0)",
                    "",
                    "",
                    f"=IFERROR(Y{sheet2_line}/X{sheet2_line};0)",
                ]
            ],
            "ROWS",
        )

        # Задание форматы вывода строки
        ss.prepare_setCellsFormats(
            f"A{sheet2_line}:Z{sheet2_line}",
            [
                [
                    {
                        "numberFormat": {
                            "type": "DATE",
                            "pattern": "dd.mm.yyyy",
                        }
                    },
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {"numberFormat": {}},
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {"numberFormat": {}},
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {"numberFormat": {}},
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {"numberFormat": {}},
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                    {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        }
                    },
                ]
            ],
        )
        # Цвет фона ячеек
        if sheet2_line % 2 != 0:
            ss.prepare_setCellsFormat(
                f"A{sheet2_line}:Z{sheet2_line}",
                {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                fields="userEnteredFormat.backgroundColor",
            )

        # Бордер
        for j in range(sheet3_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": sheet2_line - 1,
                            "endRowIndex": sheet2_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "top": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": sheet2_line - 1,
                            "endRowIndex": sheet2_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": sheet2_line - 1,
                            "endRowIndex": sheet2_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": sheet2_line - 1,
                            "endRowIndex": sheet2_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
        # ss.runPrepared()
        sheet2_line += 1
        dateline += timedelta(1)

    # ИТОГО
    ss.prepare_setValues(
        f"A{sheet2_line}:Z{sheet2_line}",
        [
            [
                "ИТОГО",
                "",
                f"=SUM(C3:C{sheet2_line - 1})",
                f"=SUM(D3:D{sheet2_line - 1})",
                f"=SUM(E3:E{sheet2_line - 1})",
                f"=SUM(F3:F{sheet2_line - 1})",
                f"=IFERROR(E{sheet2_line}/C{sheet2_line};0)",
                f"=IFERROR(F{sheet2_line}/D{sheet2_line};0)",
                f"=SUM(I3:I{sheet2_line - 1})",
                f"=SUM(J3:J{sheet2_line - 1})",
                f"=IFERROR(J{sheet2_line}/I{sheet2_line};0)",
                f"=SUM(L3:L{sheet2_line - 1})",
                f"=SUM(M3:M{sheet2_line - 1})",
                f"=IFERROR(M{sheet2_line}/L{sheet2_line};0)",
                f"=SUM(O3:O{sheet2_line - 1})",
                f"=SUM(P3:P{sheet2_line - 1})",
                f"=IFERROR(P{sheet2_line}/O{sheet2_line};0)",
                f"=SUM(R3:R{sheet2_line - 1})",
                f"=SUM(S3:S{sheet2_line - 1})",
                f"=IFERROR(S{sheet2_line}/R{sheet2_line};0)",
                f"=SUM(U3:U{sheet2_line - 1})",
                f"=SUM(V3:V{sheet2_line - 1})",
                f"=IFERROR(V{sheet2_line}/U{sheet2_line};0)",
                f"=SUM(X3:X{sheet2_line - 1})",
                f"=SUM(Y3:Y{sheet2_line - 1})",
                f"=IFERROR(Y{sheet2_line}/X{sheet2_line};0)",
            ]
        ],
        "ROWS",
    )

    # Задание форматы вывода строки
    ss.prepare_setCellsFormats(
        f"A{sheet2_line}:Z{sheet2_line}",
        [
            [
                {
                    "numberFormat": {
                        "type": "DATE",
                        "pattern": "dd.mm.yyyy",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {},
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
                {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00[$ ₽]",
                    },
                    "horizontalAlignment": "RIGHT",
                    "textFormat": {"bold": True},
                },
            ]
        ],
    )
    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        f"A{sheet2_line}:Z{sheet2_line}",
        {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    for j in range(sheet3_width):
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": sheet2_line - 1,
                        "endRowIndex": sheet2_line,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "top": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0},
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": sheet2_line - 1,
                        "endRowIndex": sheet2_line,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "right": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": sheet2_line - 1,
                        "endRowIndex": sheet2_line,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "left": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": sheet2_line - 1,
                        "endRowIndex": sheet2_line,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
    ss.runPrepared()

    # ЛИСТ 4
    logging.info(
        f"{__name__}: {str(datetime.now())[:-7]}:    "
        f"Создание листа 4 в файле GoogleSheets..."
    )
    sheetId = 3
    # Ширина столбцов
    ss = Spreadsheet(
        spreadsheet["spreadsheetId"],
        sheetId,
        googleservice,
        spreadsheet["sheets"][sheetId]["properties"]["title"],
    )
    ss.prepare_setColumnWidth(0, 300)
    ss.prepare_setColumnsWidth(1, 2, 160)

    ss.prepare_setValues(
        "A1:C1",
        [
            [
                '=JOIN(" ";"Итоговый отчет будет сформирован через";DATEDIF(TODAY();DATE(YEAR(TODAY());'
                'MONTH(TODAY())+1;1)-1;"D");IF(MOD(DATEDIF(TODAY();DATE(YEAR(TODAY());MONTH(TODAY())+1;1)-1;'
                '"D");10)<5;"дня";"дней"))',
                "",
                "",
            ],
        ],
        "ROWS",
    )
    # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

    ss.prepare_setCellsFormats(
        "A1:C1",
        [
            [
                {"textFormat": {"bold": True}},
                {"textFormat": {"bold": True}},
                {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "RIGHT",
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00%",
                    },
                },
            ]
        ],
    )
    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        "A1:C1",
        {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    i = 0
    for j in range(sheet4_width):
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "top": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0},
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "right": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "left": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
    ss.runPrepared()

    # ЛИСТ 5
    logging.info(
        f"{__name__}: {str(datetime.now())[:-7]}:    "
        f"Создание листа 5 в файле GoogleSheets..."
    )
    sheetId = 4
    # Ширина столбцов
    ss = Spreadsheet(
        spreadsheet["spreadsheetId"],
        sheetId,
        googleservice,
        spreadsheet["sheets"][sheetId]["properties"]["title"],
    )
    ss.prepare_setColumnWidth(0, 300)
    ss.prepare_setColumnsWidth(1, 2, 160)

    ss.prepare_setValues(
        "A1:C1",
        [
            [
                '=JOIN(" ";"Итоговый отчет платежного агента будет сформирован через";'
                "DATEDIF(TODAY();DATE(YEAR(TODAY());"
                'MONTH(TODAY())+1;1)-1;"D");IF(MOD(DATEDIF(TODAY();DATE(YEAR(TODAY());'
                "MONTH(TODAY())+1;1)-1;"
                '"D");10)<5;"дня";"дней"))',
                "",
                "",
            ],
        ],
        "ROWS",
    )
    # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

    ss.prepare_setCellsFormats(
        "A1:C1",
        [
            [
                {"textFormat": {"bold": True}},
                {"textFormat": {"bold": True}},
                {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "RIGHT",
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "#,##0.00%",
                    },
                },
            ]
        ],
    )
    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        "A1:C1",
        {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    i = 0
    for j in range(sheet4_width):
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "top": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0, "green": 0, "blue": 0},
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "right": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "left": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
        ss.requests.append(
            {
                "updateBorders": {
                    "range": {
                        "sheetId": ss.sheetId,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": j,
                        "endColumnIndex": j + 1,
                    },
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {
                            "red": 0,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1.0,
                        },
                    },
                }
            }
        )
    ss.runPrepared()

    # ЛИСТ 6
    logging.info(
        f"{__name__}: {str(datetime.now())[:-7]}:    "
        f"Создание листа 6 в файле GoogleSheets..."
    )
    sheetId = 5
    # Ширина столбцов
    ss = Spreadsheet(
        spreadsheet["spreadsheetId"],
        sheetId,
        googleservice,
        spreadsheet["sheets"][sheetId]["properties"]["title"],
    )
    ss.prepare_setColumnsWidth(0, 1, 105)
    ss.prepare_setColumnsWidth(2, 5, 120)
    ss.prepare_setColumnWidth(6, 100)
    ss.prepare_setColumnWidth(7, 65)
    ss.prepare_setColumnWidth(8, 120)
    ss.prepare_setColumnWidth(9, 100)
    ss.prepare_setColumnWidth(10, 65)
    ss.prepare_setColumnWidth(11, 120)
    ss.prepare_setColumnWidth(12, 100)
    ss.prepare_setColumnWidth(13, 65)
    ss.prepare_setColumnWidth(14, 120)
    ss.prepare_setColumnWidth(15, 100)

    # Объединение ячеек
    ss.prepare_mergeCells("A1:A2")
    ss.prepare_mergeCells("B1:B2")
    ss.prepare_mergeCells("C1:C2")
    ss.prepare_mergeCells("D1:D2")
    ss.prepare_mergeCells("E1:E2")
    ss.prepare_mergeCells("F1:F2")
    ss.prepare_mergeCells("G1:G2")
    ss.prepare_mergeCells("H1:J1")
    ss.prepare_mergeCells("K1:M1")
    ss.prepare_mergeCells("N1:P1")

    # Задание параметров группе ячеек
    # Жирный, по центру
    ss.prepare_setCellsFormat(
        "A1:P2",
        {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
    )
    # ss.prepare_setCellsFormat('E4:E8', {'numberFormat': {'pattern': '[h]:mm:ss', 'type': 'TIME'}},
    #                           fields='userEnteredFormat.numberFormat')

    # Заполнение таблицы
    ss.prepare_setValues(
        "A1:P2",
        [
            [
                "Дата",
                "День недели",
                "Кол-во проходов\n ПЛАН",
                "Кол-во проходов\n ФАКТ",
                "Общая сумма\n ПЛАН",
                "Общая сумма\n ФАКТ",
                "Депозит",
                "Карты",
                "",
                "",
                "Услуги",
                "",
                "",
                "Товары",
                "",
                "",
            ],
            [
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
                "Кол-во",
                "Сумма",
                "Средний чек",
            ],
        ],
        "ROWS",
    )
    # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

    # Цвет фона ячеек
    ss.prepare_setCellsFormat(
        "A1:P2",
        {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
        fields="userEnteredFormat.backgroundColor",
    )

    # Бордер
    for i in range(2):
        for j in range(sheet6_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "top": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": i,
                            "endRowIndex": i + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {
                                "red": 0,
                                "green": 0,
                                "blue": 0,
                                "alpha": 1.0,
                            },
                        },
                    }
                }
            )

    ss.runPrepared()

    google_doc = (
        date_from.strftime("%Y-%m"),
        spreadsheet["spreadsheetId"],
    )

    return google_doc
