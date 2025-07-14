import logging
import re
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

import apiclient
import httplib2
from dateutil.relativedelta import relativedelta
from fastapi.exceptions import HTTPException
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel
from starlette import status

from constants import GOOGLE_DOC_VERSION
from core.settings import settings
from db.mssql import MsSqlDatabase
from legacy import functions
from legacy.to_google_sheets import Spreadsheet, create_new_google_doc
from repositories.yandex import YandexRepository, get_yandex_repo
from schemas.bars import ClientsCount
from schemas.google_report_ids import GoogleReportIdCreate
from services.bars import BarsService, get_bars_service
from services.report_config import ReportConfigService, get_report_config_service
from services.rk import RKService, get_rk_service
from services.settings import SettingsService, get_settings_service
from sql.clients_count import CLIENTS_COUNT_SQL


logger = logging.getLogger("barsicreport2")


AQUA_COMPANIES_IDS = (36, 7203673, 7203674, 13240081, 15826592, 16049033)


class DBName(StrEnum):
    """Название базы данных."""

    AQUA = "aqua"
    BEACH = "beach"


class Company(BaseModel):
    """Информация об организации."""

    id: int
    name: str
    db_name: DBName


class BarsicReport2Service:
    """
    Функционал предыдущей версии.
    """

    def __init__(self, bars_srv: MsSqlDatabase, rk_srv: MsSqlDatabase):
        self.bars_srv = bars_srv
        self.rk_srv = rk_srv

        self.count_sql_error = 0
        self.org_for_finreport = {}
        self.orgs = []
        self.new_agentservice = []
        self.agentorgs = []

        self._report_config_service: ReportConfigService = get_report_config_service()
        self._settings_service: SettingsService = get_settings_service()
        self._bars_service: BarsService = get_bars_service()
        self._rk_service: RKService = get_rk_service()
        self._yandex_repo: YandexRepository = get_yandex_repo()

    def get_clients_count(self) -> list[ClientsCount]:
        """Получение количества человек в зоне."""

        self.bars_srv.set_database(settings.mssql_database1)
        with self.bars_srv as connect:
            cursor = connect.cursor()
            cursor.execute(CLIENTS_COUNT_SQL)
            rows = cursor.fetchall()
            if not rows:
                return [ClientsCount(count=0, id=488, zone_name="", code="0003")]

        return [
            ClientsCount(count=row[0], id=row[1], zone_name=row[2], code=row[3])
            for row in rows
        ]

    def count_clients_print(self):
        """Получение количества человек в зоне Аквазоны."""

        aqua_company = self.get_companies()[0]
        clients_count = self.get_clients_count()
        self.bars_srv.set_database(settings.mssql_database1)
        with self.bars_srv as connect:
            total_report = functions.get_total_report(
                connect=connect,
                org=aqua_company.id,
                org_name=aqua_company.name,
                date_from=datetime.now(),
                date_to=datetime.now() + timedelta(1),
            )
        try:
            count_clients = int(total_report["Аквазона"][0])
        except KeyError:
            count_clients = 0

        try:
            count_clients_allday = self.reportClientCountTotals(
                database=settings.mssql_database1,
                org=aqua_company.id,
                date_from=datetime.now(),
                date_to=datetime.now() + timedelta(1),
            )[0][1]
        except IndexError:
            count_clients_allday = 0

        return {
            "Всего": str(count_clients) + " / " + str(count_clients_allday),
            clients_count[0].zone_name: clients_count[0].count,
        }

    def get_companies(self) -> list[Company]:
        """Получение списка организаций из баз данных Аквапарка и Пляжа."""

        aqua_companies_db = self.list_organisation(database=settings.mssql_database1)
        aqua_companies_map = {
            company_db[0]: company_db[2] for company_db in aqua_companies_db
        }
        companies = [
            Company(id=id_, name=name, db_name=DBName.AQUA)
            for id_, name in aqua_companies_map.items()
            if id_ in AQUA_COMPANIES_IDS
        ]

        beach_companies_db = self.list_organisation(database=settings.mssql_database2)
        companies.append(
            Company(
                id=beach_companies_db[0][0],
                name=beach_companies_db[0][2],
                db_name=DBName.BEACH,
            )
        )

        return companies

    def list_organisation(
        self,
        database,
    ):
        """Функция делает запрос в базу Барс и возвращает список заведенных
        в базе организаций в виде списка кортежей."""

        self.bars_srv.set_database(database)
        with self.bars_srv as connect:
            cursor = connect.cursor()
            id_type = 1
            cursor.execute(
                f"""
                SELECT
                    SuperAccountId, Type, Descr, CanRegister, CanPass, IsStuff, IsBlocked, 
                    BlockReason, DenyReturn, ClientCategoryId, DiscountCard, PersonalInfoId, 
                    Address, Inn, ExternalId, RegisterTime,LastTransactionTime, 
                    LegalEntityRelationTypeId, SellServicePointId, DepositServicePointId, 
                    AllowIgnoreStoredPledge, Email, Latitude, Longitude, Phone, WebSite, 
                    TNG_ProfileId
                FROM
                    SuperAccount
                WHERE
                    Type={id_type}
                """
            )
            rows = cursor.fetchall()

        return rows

    def reportClientCountTotals(
        self,
        database,
        org,
        date_from,
        date_to,
    ):
        date_from = date_from.strftime("%Y%m%d 00:00:00")
        date_to = date_to.strftime("%Y%m%d 00:00:00")

        self.bars_srv.set_database(database)
        with self.bars_srv as connect:
            cursor = connect.cursor()
            cursor.execute(
                f"exec sp_reportClientCountTotals @sa={org},@from='{date_from}',@to='{date_to}',@categoryId=0"
            )
            rows = cursor.fetchall()

        return rows

    def client_count_totals_period(
        self,
        database,
        org,
        org_name,
        date_from,
        date_to,
    ):
        """
        Если выбран 1 день возвращает словарь количества людей за текущий месяц,
        если выбран период возвращает словарь количества людей за период
        """
        count = []

        if date_from + timedelta(1) == date_to:
            first_day = datetime.strptime((date_from.strftime("%Y%m") + "01"), "%Y%m%d")
            count.append((org_name, 1))
        else:
            first_day = date_from
            count.append((org_name, 0))
        total = 0
        while first_day < date_to:
            client_count = self.reportClientCountTotals(
                database=database,
                org=org,
                date_from=first_day,
                date_to=first_day + timedelta(1),
            )
            try:
                count.append((client_count[0][0], client_count[0][1]))
                total += client_count[0][1]
            except IndexError:
                count.append((first_day, 0))
            first_day += timedelta(1)
        count.append(("Итого", total))
        return count

    def cash_report_request(
        self,
        database,
        date_from,
        date_to,
    ):
        """Делает запрос в базу Барс и возвращает суммовой отчет за запрашиваемый период."""

        date_from = date_from.strftime("%Y%m%d 00:00:00")
        date_to = date_to.strftime("%Y%m%d 00:00:00")

        self.bars_srv.set_database(database)
        with self.bars_srv as connect:
            cursor = connect.cursor()
            cursor.execute(
                f"exec sp_reportCashDeskMoney @from='{date_from}', @to='{date_to}'"
            )
            rows = cursor.fetchall()

        return rows

    def service_point_request(
        self,
        database,
    ):
        """Делает запрос в базу Барс и возвращает список рабочих мест."""

        self.bars_srv.set_database(database)
        with self.bars_srv as connect:
            cursor = connect.cursor()
            cursor.execute(
                """
                    SELECT
                        ServicePointId, Name, SuperAccountId, Type, Code, IsInternal
                    FROM 
                        ServicePoint
                """
            )
            rows = cursor.fetchall()

        return rows

    def cashdesk_report(
        self,
        companies: list[Company],
        database,
        date_from,
        date_to,
    ):
        """
        Преобразует запросы из базы в суммовой отчет
        :return: dict
        """
        cash_report = self.cash_report_request(
            database=database,
            date_from=date_from,
            date_to=date_to,
        )
        service_point = self.service_point_request(
            database=database,
        )
        service_point_dict = {}
        for point in service_point:
            service_point_dict[point[0]] = (
                point[1],
                point[2],
                point[3],
                point[4],
                point[5],
            )
        report = {}
        for line in cash_report:
            report[line[8]] = []
        for line in cash_report:
            report[line[8]].append(
                [
                    service_point_dict[line[0]][0],
                    line[1],
                    line[2],
                    line[3],
                    line[4],
                    line[5],
                    line[6],
                    line[7],
                ]
            )
        all_sum = [
            "Итого по отчету",
            Decimal(0.0),
            Decimal(0.0),
            Decimal(0.0),
            Decimal(0.0),
            Decimal(0.0),
            Decimal(0.0),
            Decimal(0.0),
        ]
        for typpe in report:
            type_sum = [
                "Итого",
                Decimal(0.0),
                Decimal(0.0),
                Decimal(0.0),
                Decimal(0.0),
                Decimal(0.0),
                Decimal(0.0),
                Decimal(0.0),
            ]
            for line in report[typpe]:
                i = 0
                while True:
                    i += 1
                    try:
                        type_sum[i] += line[i]
                        all_sum[i] += line[i]
                    except IndexError:
                        break
            report[typpe].append(type_sum)
        report["Итого"] = [all_sum]
        report["Дата"] = [[date_from, date_to]]
        if database == settings.mssql_database1:
            report["Организация"] = [[companies[0].id]]
        elif database == settings.mssql_database2:
            beach_company = next(
                company for company in companies if company.db_name == DBName.BEACH
            )
            report["Организация"] = [[beach_company.name]]
        return report

    def create_fin_report(self) -> dict:
        """Форминует финансовый отчет в установленном формате"""

        logger.info("Формирование финансового отчета")
        fin_report = {}
        is_aquazona = None

        for org, services in self.orgs_dict.items():
            if org != "Не учитывать":
                fin_report[org] = [0, 0.00]
                for serv in services:
                    itog_report_aqua = self.itog_reports[0]
                    try:
                        if org == "Дата":
                            fin_report[org][0] = itog_report_aqua[serv][0]
                            fin_report[org][1] = itog_report_aqua[serv][1]

                        elif serv == "Депозит":
                            fin_report[org][1] += itog_report_aqua[serv][1]

                        elif serv == "Аквазона":
                            fin_report["Кол-во проходов"] = [
                                itog_report_aqua[serv][0],
                                0,
                            ]
                            fin_report[org][1] += itog_report_aqua[serv][1]
                            is_aquazona = True

                        elif serv == "Организация":
                            pass

                        else:
                            for itog_report in self.itog_reports:
                                if (
                                    itog_report.get(serv)
                                    and itog_report[serv][1] != 0.0
                                ):
                                    fin_report[org][0] += itog_report[serv][0]
                                    fin_report[org][1] += itog_report[serv][1]

                    except KeyError:
                        pass
                    except TypeError:
                        pass

        if not is_aquazona:
            fin_report["Кол-во проходов"] = [0, 0.00]

        fin_report.setdefault("Online Продажи", [0, 0.0])
        fin_report["Online Продажи"][0] += self.report_bitrix[0]
        fin_report["Online Продажи"][1] += self.report_bitrix[1]

        fin_report["Смайл"][0] = self.smile_report.total_count
        fin_report["Смайл"][1] = self.smile_report.total_sum

        total_cashdesk_report = self.cashdesk_report_org1["Итого"][0]
        fin_report["MaxBonus"] = (
            0,
            float(total_cashdesk_report[6] - total_cashdesk_report[7]),
        )
        return fin_report

    def create_fin_report_last_year(self) -> dict:
        """Форминует финансовый отчет за прошлый год в установленном формате."""

        logger.info("Формирование финансового отчета за прошлый год")
        fin_report_last_year = {}
        is_aquazona = None

        for org, services in self.orgs_dict.items():
            if org != "Не учитывать":
                fin_report_last_year[org] = [0, 0.00]
                for serv in services:
                    itog_report_aqua_lastyear = self.itog_reports_lastyear[0]
                    try:
                        if org == "Дата":
                            fin_report_last_year[org][0] = itog_report_aqua_lastyear[
                                serv
                            ][0]
                            fin_report_last_year[org][1] = itog_report_aqua_lastyear[
                                serv
                            ][1]
                        elif serv == "Депозит":
                            fin_report_last_year[org][1] += itog_report_aqua_lastyear[
                                serv
                            ][1]
                        elif serv == "Аквазона":
                            fin_report_last_year["Кол-во проходов"] = [
                                itog_report_aqua_lastyear[serv][0],
                                0,
                            ]
                            fin_report_last_year[org][1] += itog_report_aqua_lastyear[
                                serv
                            ][1]
                            is_aquazona = True

                        elif serv == "Организация":
                            pass

                        else:
                            for itog_report in self.itog_reports_lastyear:
                                if (
                                    itog_report.get(serv)
                                    and itog_report[serv][1] != 0.0
                                ):
                                    fin_report_last_year[org][0] += itog_report[serv][0]
                                    fin_report_last_year[org][1] += itog_report[serv][1]

                    except KeyError:
                        pass

                    except TypeError:
                        pass

        if not is_aquazona:
            fin_report_last_year["Кол-во проходов"] = [0, 0.00]

        fin_report_last_year.setdefault("Online Продажи", [0, 0.0])
        fin_report_last_year["Online Продажи"][0] += self.report_bitrix_lastyear[0]
        fin_report_last_year["Online Продажи"][1] += self.report_bitrix_lastyear[1]
        fin_report_last_year["Смайл"][0] = self.smile_report_lastyear.total_count
        fin_report_last_year["Смайл"][1] = self.smile_report_lastyear.total_sum

        total_cashdesk_report = self.cashdesk_report_org1_lastyear["Итого"][0]
        fin_report_last_year["MaxBonus"] = (
            0,
            total_cashdesk_report[6] - total_cashdesk_report[7],
        )

        return fin_report_last_year

    def create_fin_report_beach(self) -> dict:
        """Форминует финансовый отчет по пляжу в установленном формате"""

        logger.info("Формирование финансового отчета по пляжу")
        fin_report_beach = {
            "Депозит": (0, 0),
            "Товары": (0, 0),
            "Услуги": (0, 0),
            "Карты": (0, 0),
            "Итого по отчету": (0, 0),
        }
        for service in self.itog_report_beach:
            if service == "Дата":
                fin_report_beach[service] = (
                    self.itog_report_beach[service][0],
                    self.itog_report_beach[service][1],
                )
            elif service == "Выход с пляжа":
                fin_report_beach[service] = (
                    self.itog_report_beach[service][0],
                    self.itog_report_beach[service][1],
                )
            elif not self.itog_report_beach[service][3] in fin_report_beach:
                fin_report_beach[self.itog_report_beach[service][3]] = (
                    self.itog_report_beach[service][0],
                    self.itog_report_beach[service][1],
                )
            else:
                try:
                    fin_report_beach[self.itog_report_beach[service][3]] = (
                        fin_report_beach[self.itog_report_beach[service][3]][0]
                        + self.itog_report_beach[service][0],
                        fin_report_beach[self.itog_report_beach[service][3]][1]
                        + self.itog_report_beach[service][1],
                    )
                except TypeError:
                    pass

        if "Выход с пляжа" not in fin_report_beach:
            fin_report_beach["Выход с пляжа"] = 0, 0

        return fin_report_beach

    def create_payment_agent_report(
        self, total_report: dict[str, tuple], aqua_company: Company
    ) -> dict[str, list]:
        """Форминует отчет платежного агента в установленном формате."""

        result = {}
        result["Организация"] = [aqua_company.id, aqua_company.name]
        for org, services in self.agent_dict.items():
            if org == "Не учитывать":
                continue

            result[org] = [0, 0]
            for service in services:
                try:
                    if org == "Дата":
                        result[org][0] = total_report[service][0]
                        result[org][1] = total_report[service][1]
                    elif service == "Депозит":
                        result[org][1] += total_report[service][1]
                    elif service == "Аквазона":
                        result[org][1] += total_report[service][1]
                    elif service == "Организация":
                        pass
                    else:
                        result[org][0] += total_report[service][0]
                        result[org][1] += total_report[service][1]
                except KeyError:
                    pass
                except TypeError:
                    pass

        return result

    async def export_to_google_sheet(
        self, date_from, http_auth, googleservice, fin_report: dict
    ):
        """
        Формирование и заполнение google-таблицы
        """
        logger.info("Сохранение Финансового отчета в Google-таблицах...")
        self.sheet_width = 73
        self.sheet2_width = 3
        self.sheet3_width = 26
        self.sheet4_width = 3
        self.sheet5_width = 3
        self.sheet6_width = 16
        self.sheet_height = 40
        self.sheet2_height = 40
        self.sheet4_height = 300
        self.sheet5_height = 300
        self.sheet6_height = 40

        self.data_report = datetime.strftime(fin_report["Дата"][0], "%m")
        month = [
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
        self.data_report = month[int(self.data_report)]

        doc_name = (
            f"{datetime.strftime(fin_report['Дата'][0], '%Y-%m')} "
            f"({self.data_report}) - Финансовый отчет по Аквапарку"
        )

        if fin_report["Дата"][0] + timedelta(1) != fin_report["Дата"][1]:
            logger.info("Экспорт отчета в Google Sheet за несколько дней невозможен!")
        else:
            google_report_id = (
                await self._report_config_service.get_financial_doc_id_by_date(
                    date_from
                )
            )
            if google_report_id is None:
                google_doc = create_new_google_doc(
                    googleservice=googleservice,
                    doc_name=doc_name,
                    data_report=self.data_report,
                    finreport_dict=fin_report,
                    http_auth=http_auth,
                    date_from=date_from,
                    sheet_width=self.sheet_width,
                    sheet2_width=self.sheet2_width,
                    sheet3_width=self.sheet3_width,
                    sheet4_width=self.sheet4_width,
                    sheet5_width=self.sheet5_width,
                    sheet6_width=self.sheet6_width,
                    sheet_height=self.sheet_height,
                    sheet2_height=self.sheet2_height,
                    sheet4_height=self.sheet4_height,
                    sheet5_height=self.sheet5_height,
                    sheet6_height=self.sheet6_height,
                )
                google_report_id = GoogleReportIdCreate(
                    month=google_doc[0],
                    doc_id=google_doc[1],
                    report_type="financial",
                    version=GOOGLE_DOC_VERSION,
                )
                await self._report_config_service.add_google_report_id(google_report_id)
                logger.info(f"Создана новая таблица с Id: {google_report_id.doc_id}")

            if google_report_id.version != GOOGLE_DOC_VERSION:
                error_message = (
                    f"Версия Финансового отчета ({google_report_id.version}) не соответствует текущей "
                    f"({GOOGLE_DOC_VERSION}).\n"
                    f"Необходимо сначала удалить ссылку на старую версию, "
                    f"затем заново сформировать отчет с начала месяца."
                )
                logger.error(error_message)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=error_message,
                )

            google_doc = (google_report_id.month, google_report_id.doc_id)
            self.spreadsheet = (
                googleservice.spreadsheets()
                .get(spreadsheetId=google_doc[1], ranges=[], includeGridData=True)
                .execute()
            )

            # -------------------------------- ЗАПОЛНЕНИЕ ДАННЫМИ ------------------------------------------------

            # Проверка нет ли текущей даты в таблице
            logger.info("Проверка нет ли текущей даты в таблице...")
            self.start_line = 1
            self.reprint = 2

            for line_table in self.spreadsheet["sheets"][0]["data"][0]["rowData"]:
                try:
                    if line_table["values"][0]["formattedValue"] == datetime.strftime(
                        fin_report["Дата"][0], "%d.%m.%Y"
                    ):
                        self.rewrite_google_sheet(
                            googleservice,
                            fin_report=self.fin_report,
                            fin_report_last_year=self.fin_report_last_year,
                            fin_report_beach=self.fin_report_beach,
                        )
                        self.reprint = 0
                        break
                    elif line_table["values"][0]["formattedValue"] == "ИТОГО":
                        break
                    else:
                        self.start_line += 1
                except KeyError:
                    self.start_line += 1
            if self.reprint:
                self.write_google_sheet(
                    googleservice,
                    fin_report=self.fin_report,
                    fin_report_last_year=self.fin_report_last_year,
                    fin_report_beach=self.fin_report_beach,
                )
            # width_table = len(self.spreadsheet['sheets'][0]['data'][0]['rowData'][0]['values'])
        return True

    def rewrite_google_sheet(
        self,
        googleservice,
        fin_report: dict,
        fin_report_last_year: dict,
        fin_report_beach: dict,
    ):
        """
        Заполнение google-таблицы в случае, если данные уже существуют
        """
        logger.warning("Перезапись уже существующей строки...")
        self.reprint = 1
        self.write_google_sheet(
            googleservice,
            fin_report=fin_report,
            fin_report_last_year=fin_report_last_year,
            fin_report_beach=fin_report_beach,
        )

    def write_google_sheet(
        self,
        googleservice,
        fin_report: dict,
        fin_report_last_year: dict,
        fin_report_beach: dict,
    ):
        """Заполнение google-таблицы"""
        # SHEET 1
        logger.info("Заполнение листа 1...")
        sheetId = 0
        ss = Spreadsheet(
            self.spreadsheet["spreadsheetId"],
            sheetId,
            googleservice,
            self.spreadsheet["sheets"][sheetId]["properties"]["title"],
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
        self.nex_line = self.start_line

        control_total_sum = sum(
            [
                fin_report["Билеты аквапарка"][1],
                fin_report["Общепит"][1],
                fin_report["Билеты аквапарка КОРП"][1],
                fin_report["Прочее"][1],
                fin_report["Сопутствующие товары"][1],
                fin_report["Депозит"][1],
                fin_report["Штраф"][1],
                fin_report["Online Продажи"][1],
                fin_report["Фотоуслуги"][1],
                fin_report["УЛËТSHOP"][1],
                fin_report["Аренда полотенец"][1],
                fin_report["Фишпиллинг"][1],
                fin_report["Нулевые"][1],
            ]
        )

        if fin_report["ИТОГО"][1] != control_total_sum:
            logger.error("Несоответствие данных: Сумма услуг не равна итоговой сумме")
            logger.info(
                f"Несоответствие данных: Сумма услуг по группам + депозит ({control_total_sum}) "
                f"не равна итоговой сумме ({fin_report['ИТОГО'][1]}). \n"
                f"Рекомендуется проверить правильно ли разделены услуги по группам.",
            )

        ss.prepare_setValues(
            f"A{self.nex_line}:BU{self.nex_line}",
            [
                [
                    datetime.strftime(fin_report["Дата"][0], "%d.%m.%Y"),
                    weekday_rus[fin_report["Дата"][0].weekday()],
                    f"='План'!C{self.nex_line}",
                    f"{fin_report['Кол-во проходов'][0]}",
                    f"{fin_report_last_year['Кол-во проходов'][0]}",
                    f"='План'!E{self.nex_line}",
                    f"={str(fin_report['ИТОГО'][1]).replace('.', ',')}"
                    f"-I{self.nex_line}+BT{self.nex_line}+BU{self.nex_line}+'Смайл'!C{self.nex_line}",
                    f"=IFERROR(G{self.nex_line}/D{self.nex_line};0)",
                    f"={str(fin_report['MaxBonus'][1]).replace('.', ',')}",
                    f"={str(fin_report_last_year['ИТОГО'][1]).replace('.', ',')}"
                    f"-{str(fin_report_last_year['MaxBonus'][1]).replace('.', ',')}"
                    f"+{str(fin_report_last_year['Online Продажи'][1]).replace('.', ',')}",
                    fin_report["Билеты аквапарка"][0],
                    fin_report["Билеты аквапарка"][1],
                    f"=IFERROR(L{self.nex_line}/K{self.nex_line};0)",
                    fin_report["Депозит"][1],
                    fin_report["Штраф"][1],
                    # Общепит
                    f"='План'!I{self.nex_line}",
                    f"='План'!J{self.nex_line}",
                    f"=IFERROR(Q{self.nex_line}/P{self.nex_line};0)",
                    fin_report["Общепит"][0] + fin_report["Смайл"][0],
                    fin_report["Общепит"][1] + fin_report["Смайл"][1],
                    f"=IFERROR(T{self.nex_line}/S{self.nex_line};0)",
                    fin_report_last_year["Общепит"][0]
                    + fin_report_last_year["Смайл"][0],
                    fin_report_last_year["Общепит"][1]
                    + fin_report_last_year["Смайл"][1],
                    f"=IFERROR(W{self.nex_line}/V{self.nex_line};0)",
                    # Фотоуслуги
                    f"='План'!L{self.nex_line}",
                    f"='План'!M{self.nex_line}",
                    f"=IFERROR(Z{self.nex_line}/Y{self.nex_line};0)",
                    fin_report["Фотоуслуги"][0],
                    fin_report["Фотоуслуги"][1],
                    f"=IFERROR(AC{self.nex_line}/AB{self.nex_line};0)",
                    fin_report_last_year["Фотоуслуги"][0],
                    fin_report_last_year["Фотоуслуги"][1],
                    f"=IFERROR(AF{self.nex_line}/AE{self.nex_line};0)",
                    # УЛËТSHOP
                    f"='План'!O{self.nex_line}",
                    f"='План'!P{self.nex_line}",
                    f"=IFERROR(AI{self.nex_line}/AH{self.nex_line};0)",
                    fin_report["УЛËТSHOP"][0],
                    fin_report["УЛËТSHOP"][1],
                    f"=IFERROR(AL{self.nex_line}/AK{self.nex_line};0)",
                    fin_report_last_year["УЛËТSHOP"][0],
                    fin_report_last_year["УЛËТSHOP"][1],
                    f"=IFERROR(AO{self.nex_line}/AN{self.nex_line};0)",
                    # Аренда полотенец
                    f"='План'!R{self.nex_line}",
                    f"='План'!S{self.nex_line}",
                    f"=IFERROR(AR{self.nex_line}/AQ{self.nex_line};0)",
                    fin_report["Аренда полотенец"][0],
                    fin_report["Аренда полотенец"][1],
                    f"=IFERROR(AU{self.nex_line}/AT{self.nex_line};0)",
                    fin_report_last_year["Аренда полотенец"][0],
                    fin_report_last_year["Аренда полотенец"][1],
                    f"=IFERROR(AX{self.nex_line}/AW{self.nex_line};0)",
                    # Фишпиллинг
                    f"='План'!U{self.nex_line}",
                    f"='План'!V{self.nex_line}",
                    f"=IFERROR(BA{self.nex_line}/AZ{self.nex_line};0)",
                    fin_report["Фишпиллинг"][0],
                    fin_report["Фишпиллинг"][1],
                    f"=IFERROR(BD{self.nex_line}/BC{self.nex_line};0)",
                    fin_report_last_year["Фишпиллинг"][0],
                    fin_report_last_year["Фишпиллинг"][1],
                    f"=IFERROR(BG{self.nex_line}/BF{self.nex_line};0)",
                    # Билеты аквапарка КОРП
                    fin_report["Билеты аквапарка КОРП"][0],
                    fin_report["Билеты аквапарка КОРП"][1],
                    f"=IFERROR(BJ{self.nex_line}/BI{self.nex_line};0)",
                    fin_report["Прочее"][0] + fin_report["Сопутствующие товары"][0],
                    fin_report["Прочее"][1] + fin_report["Сопутствующие товары"][1],
                    fin_report["Online Продажи"][0],
                    fin_report["Online Продажи"][1],
                    f"=IFERROR(BO{self.nex_line}/BN{self.nex_line};0)",
                    # Нулевые
                    fin_report["Нулевые"][0],
                    fin_report["Нулевые"][1],
                    f"=IFERROR(BR{self.nex_line}/BQ{self.nex_line};0)",
                    0,
                    0,
                ]
            ],
            "ROWS",
        )

        # Задание форматы вывода строки
        ss.prepare_setCellsFormats(
            f"A{self.nex_line}:BU{self.nex_line}",
            [
                [
                    {
                        "numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"},
                        "horizontalAlignment": "LEFT",
                    },
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # УЛËТSHOP
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # Аренда полотенец
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # Фишпиллинг
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # Прочее
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # ONLINE ПРОДАЖИ
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # Нулевые
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # Сумма безнал
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    # Online Прочее
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                ]
            ],
        )
        # Цвет фона ячеек
        if self.nex_line % 2 != 0:
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:BU{self.nex_line}",
                {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                fields="userEnteredFormat.backgroundColor",
            )

        # Бордер
        for j in range(self.sheet_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
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
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        ss.runPrepared()

        # ------------------------------------------- Заполнение ИТОГО --------------------------------------
        # Вычисление последней строки в таблице
        logger.info("Заполнение строки ИТОГО на листе 1...")

        self.sheet2_line = 1
        for line_table in self.spreadsheet["sheets"][2]["data"][0]["rowData"]:
            try:
                if line_table["values"][0]["formattedValue"] == "ИТОГО":
                    break
                else:
                    self.sheet2_line += 1
            except KeyError:
                self.sheet2_line += 1

        for i, line_table in enumerate(
            self.spreadsheet["sheets"][0]["data"][0]["rowData"]
        ):
            try:
                if line_table["values"][0]["formattedValue"] == "ИТОГО":
                    # Если строка переписывается - итого на 1 поз вниз, если новая - на 2 поз
                    height_table = i + self.reprint
                    break
                else:
                    height_table = 4
            except KeyError:
                pass

        ss.prepare_setValues(
            f"A{height_table}:BU{height_table}",
            [
                [
                    "ИТОГО",
                    "",
                    f"=SUM(C3:C{height_table - 1})",
                    f"=SUM(D3:D{height_table - 1})",
                    f"=SUM(E3:E{height_table - 1})",
                    f"=SUM(F3:F{height_table - 1})",
                    f"=SUM(G3:G{height_table - 1})",
                    f"=IFERROR(ROUND(G{height_table}/D{height_table};2);0)",
                    f"=SUM(I3:I{height_table - 1})",
                    f"=SUM(J3:J{height_table - 1})",
                    f"=SUM(K3:K{height_table - 1})",
                    f"=SUM(L3:L{height_table - 1})",
                    f"=IFERROR(ROUND(L{height_table}/K{height_table};2);0)",
                    f"=SUM(N3:N{height_table - 1})",
                    f"=SUM(O3:O{height_table - 1})",
                    f"=SUM(P3:P{height_table - 1})",
                    f"=SUM(Q3:Q{height_table - 1})",
                    f"=IFERROR(ROUND(Q{height_table}/P{height_table};2);0)",
                    f"=SUM(S3:S{height_table - 1})",
                    f"=SUM(T3:T{height_table - 1})",
                    f"=IFERROR(ROUND(T{height_table}/S{height_table};2);0)",
                    f"=SUM(V3:V{height_table - 1})",
                    f"=SUM(W3:W{height_table - 1})",
                    f"=IFERROR(ROUND(W{height_table}/V{height_table};2);0)",
                    f"=SUM(Y3:Y{height_table - 1})",
                    f"=SUM(Z3:Z{height_table - 1})",
                    f"=IFERROR(ROUND(Z{height_table}/Y{height_table};2);0)",
                    f"=SUM(AB3:AB{height_table - 1})",
                    f"=SUM(AC3:AC{height_table - 1})",
                    f"=IFERROR(ROUND(AC{height_table}/AB{height_table};2);0)",
                    f"=SUM(AE3:AE{height_table - 1})",
                    f"=SUM(AF3:AF{height_table - 1})",
                    f"=IFERROR(ROUND(AF{height_table}/AE{height_table};2);0)",
                    f"=SUM(AH3:AH{height_table - 1})",
                    f"=SUM(AI3:AI{height_table - 1})",
                    f"=IFERROR(ROUND(AI{height_table}/AH{height_table};2);0)",
                    f"=SUM(AK3:AK{height_table - 1})",
                    f"=SUM(AL3:AL{height_table - 1})",
                    f"=IFERROR(ROUND(AL{height_table}/AK{height_table};2);0)",
                    f"=SUM(AN3:AN{height_table - 1})",
                    f"=SUM(AO3:AO{height_table - 1})",
                    f"=IFERROR(ROUND(AO{height_table}/AN{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(AR{height_table}/AQ{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(AU{height_table}/AT{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(AX{height_table}/AW{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(BA{height_table}/AZ{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(BD{height_table}/BC{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(BG{height_table}/BF{height_table};2);0)",
                    f"=SUM(AQ3:AQ{height_table - 1})",
                    f"=SUM(AR3:AR{height_table - 1})",
                    f"=IFERROR(ROUND(BJ{height_table}/BI{height_table};2);0)",
                    f"=SUM(AT3:AT{height_table - 1})",
                    f"=SUM(AU3:AU{height_table - 1})",
                    f"=SUM(AV3:AV{height_table - 1})",
                    f"=SUM(AW3:AW{height_table - 1})",
                    f"=IFERROR(ROUND(BO{height_table}/BN{height_table};2);0)",
                    f"=SUM(AV3:AV{height_table - 1})",
                    f"=SUM(AW3:AW{height_table - 1})",
                    f"=IFERROR(ROUND(BR{height_table}/BQ{height_table};2);0)",
                    f"=SUM(AY3:AY{height_table - 1})",
                    f"=SUM(AZ3:AZ{height_table - 1})",
                ]
            ],
            "ROWS",
        )
        ss.prepare_setValues(
            f"A{height_table + 1}:D{height_table + 1}",
            [
                [
                    "Выполнение плана (трафик)",
                    "",
                    f"=IFERROR('План'!C{self.sheet2_line};0)",
                    f"=IFERROR(ROUND(D{height_table}/C{height_table + 1};2);0)",
                ]
            ],
            "ROWS",
        )
        ss.prepare_setValues(
            f"A{height_table + 2}:D{height_table + 2}",
            [
                [
                    "Выполнение плана (доход)",
                    "",
                    f"=IFERROR('План'!E{self.sheet2_line};0)",
                    f"=IFERROR(ROUND(G{height_table}/C{height_table + 2};2);0)",
                ]
            ],
            "ROWS",
        )

        # Задание формата вывода строки
        ss.prepare_setCellsFormats(
            f"A{height_table}:BU{height_table}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                ]
            ],
        )
        ss.prepare_setCellsFormats(
            f"A{height_table + 1}:D{height_table + 1}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {},
                    },
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00%"},
                    },
                ]
            ],
        )
        ss.prepare_setCellsFormats(
            f"A{height_table + 2}:D{height_table + 2}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        },
                    },
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00%"},
                    },
                ]
            ],
        )

        # Цвет фона ячеек
        ss.prepare_setCellsFormat(
            f"A{height_table}:BU{height_table}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )
        ss.prepare_setCellsFormat(
            f"A{height_table + 1}:D{height_table + 1}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )
        ss.prepare_setCellsFormat(
            f"A{height_table + 2}:D{height_table + 2}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )

        # Бордер
        for j in range(self.sheet_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
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
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        for j in range(4):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
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
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        for j in range(4):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
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
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        ss.runPrepared()

        logger.info("Заполнение листа 2...")
        sheetId = 1
        ss = Spreadsheet(
            self.spreadsheet["spreadsheetId"],
            sheetId,
            googleservice,
            self.spreadsheet["sheets"][sheetId]["properties"]["title"],
        )

        self.nex_line = self.start_line

        ss.prepare_setValues(
            f"A{self.nex_line}:C{self.nex_line}",
            [
                [
                    datetime.strftime(fin_report["Дата"][0], "%d.%m.%Y"),
                    self.smile_report.total_count,
                    self.smile_report.total_sum,
                ]
            ],
            "ROWS",
        )

        # Задание форматы вывода строки
        ss.prepare_setCellsFormats(
            f"A{self.nex_line}:C{self.nex_line}",
            [
                [
                    {
                        "numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"},
                        "horizontalAlignment": "LEFT",
                    },
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                ]
            ],
        )
        # Цвет фона ячеек
        if self.nex_line % 2 != 0:
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                fields="userEnteredFormat.backgroundColor",
            )

        # Бордер
        for j in range(self.sheet2_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
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
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )

        # ------------------------------------------- Заполнение ИТОГО --------------------------------------
        # Вычисление последней строки в таблице
        logger.info("Заполнение строки ИТОГО на листе 2...")

        for i, line_table in enumerate(
            self.spreadsheet["sheets"][1]["data"][0]["rowData"]
        ):
            try:
                if line_table["values"][0]["formattedValue"] == "ИТОГО":
                    # Если строка переписывается - итого на 1 поз вниз, если новая - на 2 поз
                    height_table = i + self.reprint
                    break
                else:
                    height_table = 4
            except KeyError:
                pass

        ss.prepare_setValues(
            f"A{height_table}:C{height_table}",
            [
                [
                    "ИТОГО",
                    f"=SUM(B3:B{height_table - 1})",
                    f"=SUM(C3:C{height_table - 1})",
                ]
            ],
            "ROWS",
        )

        # Задание формата вывода строки
        ss.prepare_setCellsFormats(
            f"A{height_table}:C{height_table}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
                    {
                        "numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"},
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True},
                    },
                ]
            ],
        )

        # Цвет фона ячеек
        ss.prepare_setCellsFormat(
            f"A{height_table}:C{height_table}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )

        # Бордер
        for j in range(self.sheet2_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
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
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        ss.runPrepared()

        if self.itog_report_month:
            # SHEET 4
            logger.info("Заполнение  листа 4...")
            sheetId = 3
            ss = Spreadsheet(
                self.spreadsheet["spreadsheetId"],
                sheetId,
                googleservice,
                self.spreadsheet["sheets"][sheetId]["properties"]["title"],
            )

            self.nex_line = 1
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [["Итоговый отчет", "", ""]],
                "ROWS",
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {
                    "horizontalAlignment": "LEFT",
                    "textFormat": {"bold": True, "fontSize": 18},
                },
            )

            self.nex_line += 1
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [
                    [
                        f"За {self.data_report} {datetime.strftime(fin_report['Дата'][0], '%Y')}",
                        "",
                        "",
                    ]
                ],
                "ROWS",
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {"horizontalAlignment": "LEFT", "textFormat": {"bold": False}},
            )

            self.nex_line += 2
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [["Название", "Количество", "Сумма"]],
                "ROWS",
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {
                    "horizontalAlignment": "LEFT",
                    "textFormat": {"bold": True, "fontSize": 14},
                },
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
                fields="userEnteredFormat.backgroundColor",
            )
            for j in range(self.sheet4_width):
                ss.requests.append(
                    {
                        "updateBorders": {
                            "range": {
                                "sheetId": ss.sheetId,
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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

            for group, group_values in self.finreport_dict_month.items():
                if group == "Контрольная сумма":
                    continue
                if group == "Дата":
                    continue
                self.nex_line += 1
                ss.prepare_setValues(
                    f"A{self.nex_line}:C{self.nex_line}",
                    [
                        [
                            group,
                            group_values["Итого по группе"][0][1],
                            group_values["Итого по группе"][0][2],
                        ]
                    ],
                    "ROWS",
                )
                ss.prepare_setCellsFormats(
                    f"A{self.nex_line}:C{self.nex_line}",
                    [
                        [
                            {"textFormat": {"bold": True, "fontSize": 12}},
                            {
                                "textFormat": {"bold": True, "fontSize": 12},
                                "horizontalAlignment": "RIGHT",
                                "numberFormat": {},
                            },
                            {
                                "textFormat": {"bold": True, "fontSize": 12},
                                "horizontalAlignment": "RIGHT",
                                "numberFormat": {
                                    "type": "CURRENCY",
                                    "pattern": "#,##0.00[$ ₽]",
                                },
                            },
                        ]
                    ],
                )
                ss.prepare_setCellsFormat(
                    f"A{self.nex_line}:C{self.nex_line}",
                    {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
                    fields="userEnteredFormat.backgroundColor",
                )
                for j in range(self.sheet4_width):
                    ss.requests.append(
                        {
                            "updateBorders": {
                                "range": {
                                    "sheetId": ss.sheetId,
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                for folder, folder_values in group_values.items():
                    if folder == "Итого по группе":
                        continue
                    if folder == "":
                        continue
                    self.nex_line += 1
                    if folder is None:
                        folder_name = "Без группировки"
                    else:
                        folder_name = folder
                    ss.prepare_setValues(
                        f"A{self.nex_line}:C{self.nex_line}",
                        [
                            [
                                folder_name,
                                folder_values[0][1],
                                folder_values[0][2],
                            ]
                        ],
                        "ROWS",
                    )
                    ss.prepare_setCellsFormats(
                        f"A{self.nex_line}:C{self.nex_line}",
                        [
                            [
                                {"textFormat": {"bold": True}},
                                {
                                    "textFormat": {"bold": True},
                                    "horizontalAlignment": "RIGHT",
                                    "numberFormat": {},
                                },
                                {
                                    "textFormat": {"bold": True},
                                    "horizontalAlignment": "RIGHT",
                                    "numberFormat": {
                                        "type": "CURRENCY",
                                        "pattern": "#,##0.00[$ ₽]",
                                    },
                                },
                            ]
                        ],
                    )
                    ss.prepare_setCellsFormat(
                        f"A{self.nex_line}:C{self.nex_line}",
                        {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                        fields="userEnteredFormat.backgroundColor",
                    )
                    for j in range(self.sheet4_width):
                        ss.requests.append(
                            {
                                "updateBorders": {
                                    "range": {
                                        "sheetId": ss.sheetId,
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                    for service_name, service_count, service_sum in folder_values:
                        if service_name == "Итого по папке":
                            continue
                        self.nex_line += 1
                        ss.prepare_setValues(
                            f"A{self.nex_line}:C{self.nex_line}",
                            [[service_name, service_count, service_sum]],
                            "ROWS",
                        )
                        ss.prepare_setCellsFormats(
                            f"A{self.nex_line}:C{self.nex_line}",
                            [
                                [
                                    {"textFormat": {"bold": False}},
                                    {
                                        "textFormat": {"bold": False},
                                        "horizontalAlignment": "RIGHT",
                                        "numberFormat": {},
                                    },
                                    {
                                        "textFormat": {"bold": False},
                                        "horizontalAlignment": "RIGHT",
                                        "numberFormat": {
                                            "type": "CURRENCY",
                                            "pattern": "#,##0.00[$ ₽]",
                                        },
                                    },
                                ]
                            ],
                        )
                        for j in range(self.sheet4_width):
                            ss.requests.append(
                                {
                                    "updateBorders": {
                                        "range": {
                                            "sheetId": ss.sheetId,
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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

            while self.nex_line < self.sheet4_height:
                self.nex_line += 1
                ss.prepare_setValues(
                    f"A{self.nex_line}:C{self.nex_line}", [["", "", ""]], "ROWS"
                )
                ss.prepare_setCellsFormat(
                    f"A{self.nex_line}:C{self.nex_line}",
                    {
                        "horizontalAlignment": "LEFT",
                        "textFormat": {"bold": False, "fontSize": 10},
                    },
                )
                ss.prepare_setCellsFormat(
                    f"A{self.nex_line}:C{self.nex_line}",
                    {"backgroundColor": functions.htmlColorToJSON("#ffffff")},
                    fields="userEnteredFormat.backgroundColor",
                )
                for j in range(self.sheet4_width):
                    ss.requests.append(
                        {
                            "updateBorders": {
                                "range": {
                                    "sheetId": ss.sheetId,
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
                                    "startColumnIndex": j,
                                    "endColumnIndex": j + 1,
                                },
                                "right": {
                                    "style": "NONE",
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
                                    "startColumnIndex": j,
                                    "endColumnIndex": j + 1,
                                },
                                "left": {
                                    "style": "NONE",
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
                                    "startColumnIndex": j,
                                    "endColumnIndex": j + 1,
                                },
                                "bottom": {
                                    "style": "NONE",
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

            # SHEET 4
            logger.info("Заполнение  листа 5...")
            sheetId = 4
            ss = Spreadsheet(
                self.spreadsheet["spreadsheetId"],
                sheetId,
                googleservice,
                self.spreadsheet["sheets"][sheetId]["properties"]["title"],
            )

            self.nex_line = 1
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [["Итоговый отчет платежного агента", "", ""]],
                "ROWS",
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {
                    "horizontalAlignment": "LEFT",
                    "textFormat": {"bold": True, "fontSize": 18},
                },
            )

            self.nex_line += 1
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [
                    [
                        f"За {self.data_report} {datetime.strftime(fin_report['Дата'][0], '%Y')}",
                        "",
                        "",
                    ]
                ],
                "ROWS",
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {"horizontalAlignment": "LEFT", "textFormat": {"bold": False}},
            )

            self.nex_line += 2
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [["Название", "Количество", "Сумма"]],
                "ROWS",
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {
                    "horizontalAlignment": "LEFT",
                    "textFormat": {"bold": True, "fontSize": 14},
                },
            )
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:C{self.nex_line}",
                {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
                fields="userEnteredFormat.backgroundColor",
            )
            for j in range(self.sheet5_width):
                ss.requests.append(
                    {
                        "updateBorders": {
                            "range": {
                                "sheetId": ss.sheetId,
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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
                                "startRowIndex": self.nex_line - 1,
                                "endRowIndex": self.nex_line,
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

            for group in self.agentreport_dict_month:
                if group == "Контрольная сумма":
                    continue
                if group == "Дата":
                    continue
                if group == "Не учитывать":
                    continue
                self.nex_line += 1
                ss.prepare_setValues(
                    f"A{self.nex_line}:C{self.nex_line}",
                    [
                        [
                            group,
                            self.agentreport_dict_month[group]["Итого по группе"][0][1],
                            self.agentreport_dict_month[group]["Итого по группе"][0][2],
                        ]
                    ],
                    "ROWS",
                )
                ss.prepare_setCellsFormats(
                    f"A{self.nex_line}:C{self.nex_line}",
                    [
                        [
                            {"textFormat": {"bold": True, "fontSize": 12}},
                            {
                                "textFormat": {"bold": True, "fontSize": 12},
                                "horizontalAlignment": "RIGHT",
                                "numberFormat": {},
                            },
                            {
                                "textFormat": {"bold": True, "fontSize": 12},
                                "horizontalAlignment": "RIGHT",
                                "numberFormat": {
                                    "type": "CURRENCY",
                                    "pattern": "#,##0.00[$ ₽]",
                                },
                            },
                        ]
                    ],
                )
                ss.prepare_setCellsFormat(
                    f"A{self.nex_line}:C{self.nex_line}",
                    {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
                    fields="userEnteredFormat.backgroundColor",
                )
                for j in range(self.sheet4_width):
                    ss.requests.append(
                        {
                            "updateBorders": {
                                "range": {
                                    "sheetId": ss.sheetId,
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
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
                for folder in self.agentreport_dict_month[group]:
                    if folder == "Итого по группе":
                        continue
                    if folder == "":
                        continue
                    self.nex_line += 1
                    if folder is None:
                        folder_name = "Без группировки"
                    else:
                        folder_name = folder
                    ss.prepare_setValues(
                        f"A{self.nex_line}:C{self.nex_line}",
                        [
                            [
                                folder_name,
                                self.agentreport_dict_month[group][folder][0][1],
                                self.agentreport_dict_month[group][folder][0][2],
                            ]
                        ],
                        "ROWS",
                    )
                    ss.prepare_setCellsFormats(
                        f"A{self.nex_line}:C{self.nex_line}",
                        [
                            [
                                {"textFormat": {"bold": True}},
                                {
                                    "textFormat": {"bold": True},
                                    "horizontalAlignment": "RIGHT",
                                    "numberFormat": {},
                                },
                                {
                                    "textFormat": {"bold": True},
                                    "horizontalAlignment": "RIGHT",
                                    "numberFormat": {
                                        "type": "CURRENCY",
                                        "pattern": "#,##0.00[$ ₽]",
                                    },
                                },
                            ]
                        ],
                    )
                    ss.prepare_setCellsFormat(
                        f"A{self.nex_line}:C{self.nex_line}",
                        {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                        fields="userEnteredFormat.backgroundColor",
                    )
                    for j in range(self.sheet4_width):
                        ss.requests.append(
                            {
                                "updateBorders": {
                                    "range": {
                                        "sheetId": ss.sheetId,
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                                        "startRowIndex": self.nex_line - 1,
                                        "endRowIndex": self.nex_line,
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
                    for servise in self.agentreport_dict_month[group][folder]:
                        if servise[0] == "Итого по папке":
                            continue
                        self.nex_line += 1
                        ss.prepare_setValues(
                            f"A{self.nex_line}:C{self.nex_line}",
                            [[servise[0], servise[1], servise[2]]],
                            "ROWS",
                        )
                        ss.prepare_setCellsFormats(
                            f"A{self.nex_line}:C{self.nex_line}",
                            [
                                [
                                    {"textFormat": {"bold": False}},
                                    {
                                        "textFormat": {"bold": False},
                                        "horizontalAlignment": "RIGHT",
                                        "numberFormat": {},
                                    },
                                    {
                                        "textFormat": {"bold": False},
                                        "horizontalAlignment": "RIGHT",
                                        "numberFormat": {
                                            "type": "CURRENCY",
                                            "pattern": "#,##0.00[$ ₽]",
                                        },
                                    },
                                ]
                            ],
                        )
                        for j in range(self.sheet5_width):
                            ss.requests.append(
                                {
                                    "updateBorders": {
                                        "range": {
                                            "sheetId": ss.sheetId,
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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
                                            "startRowIndex": self.nex_line - 1,
                                            "endRowIndex": self.nex_line,
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

            while self.nex_line < self.sheet5_height:
                self.nex_line += 1
                ss.prepare_setValues(
                    f"A{self.nex_line}:C{self.nex_line}", [["", "", ""]], "ROWS"
                )
                ss.prepare_setCellsFormat(
                    f"A{self.nex_line}:C{self.nex_line}",
                    {
                        "horizontalAlignment": "LEFT",
                        "textFormat": {"bold": False, "fontSize": 10},
                    },
                )
                ss.prepare_setCellsFormat(
                    f"A{self.nex_line}:C{self.nex_line}",
                    {"backgroundColor": functions.htmlColorToJSON("#ffffff")},
                    fields="userEnteredFormat.backgroundColor",
                )
                for j in range(self.sheet5_width):
                    ss.requests.append(
                        {
                            "updateBorders": {
                                "range": {
                                    "sheetId": ss.sheetId,
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
                                    "startColumnIndex": j,
                                    "endColumnIndex": j + 1,
                                },
                                "right": {
                                    "style": "NONE",
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
                                    "startColumnIndex": j,
                                    "endColumnIndex": j + 1,
                                },
                                "left": {
                                    "style": "NONE",
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
                                    "startRowIndex": self.nex_line - 1,
                                    "endRowIndex": self.nex_line,
                                    "startColumnIndex": j,
                                    "endColumnIndex": j + 1,
                                },
                                "bottom": {
                                    "style": "NONE",
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

        # Заполнение листа 6
        logger.info("Заполнение листа 6...")
        sheetId = 5
        ss = Spreadsheet(
            self.spreadsheet["spreadsheetId"],
            sheetId,
            googleservice,
            self.spreadsheet["sheets"][sheetId]["properties"]["title"],
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
        self.nex_line = self.start_line
        ss.prepare_setValues(
            f"A{self.nex_line}:P{self.nex_line}",
            [
                [
                    datetime.strftime(fin_report_beach["Дата"][0], "%d.%m.%Y"),
                    weekday_rus[fin_report_beach["Дата"][0].weekday()],
                    f"='План'!L{self.nex_line}",
                    fin_report_beach["Выход с пляжа"][0],
                    f"='План'!M{self.nex_line}",
                    str(fin_report_beach["Итого по отчету"][1]).replace(".", ","),
                    fin_report_beach["Депозит"][1],
                    fin_report_beach["Карты"][0],
                    fin_report_beach["Карты"][1],
                    f"=IFERROR(I{self.nex_line}/H{self.nex_line};0)",
                    fin_report_beach["Услуги"][0],
                    fin_report_beach["Услуги"][1],
                    f"=IFERROR(L{self.nex_line}/K{self.nex_line};0)",
                    fin_report_beach["Товары"][0],
                    fin_report_beach["Товары"][1],
                    f"=IFERROR(O{self.nex_line}/N{self.nex_line};0)",
                ]
            ],
            "ROWS",
        )

        # Задание форматы вывода строки
        ss.prepare_setCellsFormats(
            f"A{self.nex_line}:P{self.nex_line}",
            [
                [
                    {
                        "numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"},
                        "horizontalAlignment": "LEFT",
                    },
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00[$ ₽]"}},
                ]
            ],
        )
        # Цвет фона ячеек
        if self.nex_line % 2 != 0:
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:P{self.nex_line}",
                {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                fields="userEnteredFormat.backgroundColor",
            )

        # Бордер
        for j in range(self.sheet6_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
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
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": self.nex_line - 1,
                            "endRowIndex": self.nex_line,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )

        # ------------------------------------------- Заполнение ИТОГО --------------------------------------
        logger.info("Заполнение строки ИТОГО на листе 2...")

        for i, line_table in enumerate(
            self.spreadsheet["sheets"][1]["data"][0]["rowData"]
        ):
            try:
                if line_table["values"][0]["formattedValue"] == "ИТОГО":
                    # Если строка переписывается - итого на 1 поз вниз, если новая - на 2 поз
                    height_table = i + self.reprint
                    break
                else:
                    height_table = 4
            except KeyError:
                pass

        ss.prepare_setValues(
            f"A{height_table}:P{height_table}",
            [
                [
                    "ИТОГО",
                    "",
                    f"=SUM(C3:C{height_table - 1})",
                    f"=SUM(D3:D{height_table - 1})",
                    f"=SUM(E3:E{height_table - 1})",
                    f"=SUM(F3:F{height_table - 1})",
                    f"=SUM(G3:G{height_table - 1})",
                    f"=SUM(H3:H{height_table - 1})",
                    f"=SUM(I3:I{height_table - 1})",
                    f"=IFERROR(ROUND(I{height_table}/H{height_table};2);0)",
                    f"=SUM(K3:K{height_table - 1})",
                    f"=SUM(L3:L{height_table - 1})",
                    f"=IFERROR(ROUND(L{height_table}/K{height_table};2);0)",
                    f"=SUM(N3:N{height_table - 1})",
                    f"=SUM(O3:O{height_table - 1})",
                    f"=IFERROR(ROUND(O{height_table}/N{height_table};2);0)",
                ]
            ],
            "ROWS",
        )
        ss.prepare_setValues(
            f"A{height_table + 1}:D{height_table + 1}",
            [
                [
                    "Выполнение плана (трафик)",
                    "",
                    f"=IFERROR('План'!L{self.sheet2_line};0)",
                    f"=IFERROR(ROUND(D{height_table}/C{height_table + 1};2);0)",
                ]
            ],
            "ROWS",
        )
        ss.prepare_setValues(
            f"A{height_table + 2}:D{height_table + 2}",
            [
                [
                    "Выполнение плана (доход)",
                    "",
                    f"=IFERROR('План'!M{self.sheet2_line};0)",
                    f"=IFERROR(ROUND(F{height_table}/C{height_table + 2};2);0)",
                ]
            ],
            "ROWS",
        )

        # Задание формата вывода строки
        ss.prepare_setCellsFormats(
            f"A{height_table}:P{height_table}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
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
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
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
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
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
                    {"horizontalAlignment": "RIGHT", "textFormat": {"bold": True}},
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
        ss.prepare_setCellsFormats(
            f"A{height_table + 1}:D{height_table + 1}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {},
                    },
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00%"},
                    },
                ]
            ],
        )
        ss.prepare_setCellsFormats(
            f"A{height_table + 2}:D{height_table + 2}",
            [
                [
                    {"textFormat": {"bold": True}},
                    {"textFormat": {"bold": True}},
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "#,##0.00[$ ₽]",
                        },
                    },
                    {
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "RIGHT",
                        "numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00%"},
                    },
                ]
            ],
        )

        # Цвет фона ячеек
        ss.prepare_setCellsFormat(
            f"A{height_table}:P{height_table}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )
        ss.prepare_setCellsFormat(
            f"A{height_table + 1}:D{height_table + 1}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )
        ss.prepare_setCellsFormat(
            f"A{height_table + 2}:D{height_table + 2}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )

        # Бордер
        for j in range(self.sheet6_width):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
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
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table - 1,
                            "endRowIndex": height_table,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        for j in range(4):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
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
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table,
                            "endRowIndex": height_table + 1,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        for j in range(4):
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
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
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "right": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "left": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
            ss.requests.append(
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": ss.sheetId,
                            "startRowIndex": height_table + 1,
                            "endRowIndex": height_table + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1,
                        },
                        "bottom": {
                            "style": "SOLID",
                            "width": 1,
                            "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1.0},
                        },
                    }
                }
            )
        ss.runPrepared()

    def sms_report(self, date_from, fin_report: dict) -> str:
        """Составляет текстовую версию финансового отчета."""

        logger.info("Составление SMS-отчета...")
        resporse = "Отчет по аквапарку за "

        if fin_report["Дата"][0] == fin_report["Дата"][1] - timedelta(1):
            resporse += f'{datetime.strftime(fin_report["Дата"][0], "%d.%m.%Y")}:\n'

        else:
            resporse += (
                f'{datetime.strftime(fin_report["Дата"][0], "%d.%m.%Y")} '
                f'- {datetime.strftime(fin_report["Дата"][1] - timedelta(1), "%d.%m.%Y")}:\n'
            )

        def get_sum(field_name: str) -> float:
            return fin_report.get(field_name, [0, 0])[1]

        bars_sum = get_sum("ИТОГО")
        smile = get_sum("Смайл")
        bonuses = get_sum("MaxBonus")
        other = get_sum("Прочее") + get_sum("Сопутствующие товары")
        total_sum = bars_sum - bonuses + smile

        if fin_report["ИТОГО"][1]:
            resporse += f'Люди - {fin_report["Кол-во проходов"][0]};\n'
            resporse += f"По аквапарку - {get_sum('Билеты аквапарка') + get_sum('Билеты аквапарка КОРП'):.2f} ₽;\n"
            resporse += f"По общепиту - {(get_sum('Общепит') + smile):.2f} ₽;\n"

            resporse += f"Прочее - {other:.2f} ₽;\n"
            resporse += f"Общая по БАРСу - {bars_sum:.2f} ₽;\n"
            resporse += f"Бонусы - {bonuses:.2f} ₽;\n"
            resporse += f"ONLINE продажи - {get_sum('Online Продажи'):.2f} ₽;\n"

        if not re.search(r"По общепиту", resporse) and smile:
            resporse += f"По общепиту - {smile:.2f} ₽;\n"
            resporse += f"Общая по БАРСу - {bars_sum:.2f} ₽;\n"

        resporse += f"Общая ИТОГО - {total_sum:.2f} ₽;\n\n"

        if self.itog_report_beach["Итого по отчету"][1]:
            try:
                resporse += f'Люди (пляж) - {self.itog_report_beach["Летняя зона | БЕЗЛИМИТ | 1 проход"][0]};\n'
            except KeyError:
                pass
            resporse += f'Итого по пляжу - {self.itog_report_beach["Итого по отчету"][1]:.2f} ₽;\n'

        resporse += "Без ЧП."

        with open(
            f'reports/{date_from.strftime("%Y.%m.%d")}_sms.txt', "w", encoding="utf-8"
        ) as f:
            f.write(resporse)
        return resporse

    async def save_reports(self, date_from, aqua_company: Company):
        """
        Функция управления
        """
        self.fin_report = self.create_fin_report()
        payment_agent_report = self.create_payment_agent_report(
            functions.concatenate_total_reports(
                self.itog_reports[0],
                {"Смайл": (self.smile_report.total_count, self.smile_report.total_sum)},
            ),
            aqua_company=aqua_company,
        )
        # agentreport_xls
        self.path_list.append(
            self._yandex_repo.export_payment_agent_report(
                payment_agent_report, date_from
            )
        )
        # finreport_google
        self.fin_report_last_year = self.create_fin_report_last_year()
        self.fin_report_beach = self.create_fin_report_beach()

        self.finreport_dict_month = None
        if self.itog_report_month:
            self.finreport_dict_month = functions.create_month_finance_report(
                itog_report_month=self.itog_report_month,
                itogreport_group_dict=self.itogreport_group_dict,
                orgs_dict=self.orgs_dict,
                smile_report_month=self.smile_report_month,
            )
            self.agentreport_dict_month = functions.create_month_agent_report(
                month_total_report=self.itog_report_month,
                agent_dict=self.agent_dict,
                smile_report_month=self.smile_report_month,
            )

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            settings.google_api_settings.google_service_account_config,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        httpAuth = credentials.authorize(httplib2.Http())
        try:
            logger.info("Попытка авторизации с Google-документами ...")
            googleservice = apiclient.discovery.build(
                "sheets", "v4", http=httpAuth, cache_discovery=False
            )

        except IndexError as e:
            error_message = f"Ошибка {repr(e)}"
            logger.error(error_message)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=error_message,
            )

        await self.export_to_google_sheet(
            date_from, httpAuth, googleservice, fin_report=self.fin_report
        )

        # finreport_telegram:
        self.sms_report_list.append(
            self.sms_report(date_from, fin_report=self.fin_report)
        )

        # check_itogreport_xls:
        for itog_report in self.itog_reports:
            if itog_report["Итого по отчету"][1]:
                self.path_list.append(
                    self._yandex_repo.save_organisation_total(itog_report, date_from)
                )

        if self.itog_report_beach["Итого по отчету"][1]:
            self.path_list.append(
                self._yandex_repo.save_organisation_total(
                    self.itog_report_beach, date_from
                )
            )

        # check_cashreport_xls:
        if self.cashdesk_report_org1["Итого"][0][1]:
            self.path_list.append(
                self._yandex_repo.save_cashdesk_report(
                    self.cashdesk_report_org1, date_from
                )
            )
        if self.cashdesk_report_org2["Итого"][0][1]:
            self.path_list.append(
                self._yandex_repo.save_cashdesk_report(
                    self.cashdesk_report_org2, date_from
                )
            )
        # check_client_count_total_xls:
        if self.client_count_totals_org1[-1][1]:
            self.path_list.append(
                self._yandex_repo.save_client_count_totals(
                    self.client_count_totals_org1, date_from
                )
            )
        if self.client_count_totals_org2[-1][1]:
            self.path_list.append(
                self._yandex_repo.save_client_count_totals(
                    self.client_count_totals_org2, date_from
                )
            )

    async def load_report(
        self,
        date_from,
        date_to,
        companies: list[Company],
    ):
        """Выполнить отчеты"""

        self.itog_report_beach = None
        self.report_bitrix = (0, 0)
        self.report_bitrix_lastyear = (0, 0)
        self.smile_report = self._rk_service.get_smile_report(
            date_from=date_from,
            date_to=date_to,
        )
        self.smile_report_lastyear = self._rk_service.get_smile_report(
            date_from=date_from - relativedelta(years=1),
            date_to=date_to - relativedelta(years=1),
        )

        if companies[0]:
            self.bars_srv.set_database(settings.mssql_database1)
            with self.bars_srv as connect:

                self.itog_reports = []
                self.itog_reports_lastyear = []
                for company in companies:
                    if company.db_name == DBName.BEACH:
                        continue

                    self.itog_reports.append(
                        functions.get_total_report(
                            connect=connect,
                            org=company.id,
                            org_name=company.name,
                            date_from=date_from,
                            date_to=date_to,
                        )
                    )
                    self.itog_reports_lastyear.append(
                        functions.get_total_report(
                            connect=connect,
                            org=company.id,
                            org_name=company.name,
                            date_from=date_from - relativedelta(years=1),
                            date_to=date_to - relativedelta(years=1),
                        )
                    )

            self.itog_report_month = None
            if int((date_to - timedelta(1)).strftime("%y%m")) < int(
                date_to.strftime("%y%m")
            ):
                self._bars_service.choose_db(settings.mssql_database1)
                organizations = self._bars_service.get_organisations()
                itog_report_month = {}
                for organization in organizations:
                    self.bars_srv.set_database(settings.mssql_database1)
                    with self.bars_srv as connect:
                        itog_report_month_for_org = functions.get_total_report(
                            connect=connect,
                            org=organization.super_account_id,
                            org_name=organization.descr,
                            date_from=datetime.strptime(
                                "01" + (date_to - timedelta(1)).strftime("%m%y"),
                                "%d%m%y",
                            ),
                            date_to=date_to,
                        )
                    itog_report_month = functions.concatenate_total_reports(
                        itog_report_month, itog_report_month_for_org
                    )

                self.itog_report_month = itog_report_month
                self.smile_report_month = self._rk_service.get_smile_report(
                    date_from=datetime.strptime(
                        "01" + (date_to - timedelta(1)).strftime("%m%y"), "%d%m%y"
                    ),
                    date_to=date_to,
                )

            self.cashdesk_report_org1 = self.cashdesk_report(
                database=settings.mssql_database1,
                date_from=date_from,
                date_to=date_to,
                companies=companies,
            )
            self.cashdesk_report_org1_lastyear = self.cashdesk_report(
                database=settings.mssql_database1,
                date_from=date_from - relativedelta(years=1),
                date_to=date_to - relativedelta(years=1),
                companies=companies,
            )
            self.client_count_totals_org1 = self.client_count_totals_period(
                database=settings.mssql_database1,
                org=companies[0].id,
                org_name=companies[0].name,
                date_from=date_from,
                date_to=date_to,
            )

        beach_company = next(
            company for company in companies if company.db_name == DBName.BEACH
        )
        if beach_company:
            self.bars_srv.set_database(settings.mssql_database2)
            with self.bars_srv as connect:
                self.itog_report_beach = functions.get_total_report(
                    connect=connect,
                    org=beach_company.id,
                    org_name=beach_company.name,
                    date_from=date_from,
                    date_to=date_to,
                    is_legacy_database=True,
                )

            self.cashdesk_report_org2 = self.cashdesk_report(
                database=settings.mssql_database2,
                date_from=date_from,
                date_to=date_to,
                companies=companies,
            )
            self.client_count_totals_org2 = self.client_count_totals_period(
                database=settings.mssql_database2,
                org=beach_company.id,
                org_name=beach_company.name,
                date_from=date_from,
                date_to=date_to,
            )

    async def run_report(self, date_from, date_to, use_yadisk: bool = False):
        self.path_list = []
        self.sms_report_list = []

        companies = self.get_companies()

        period = []
        while True:
            period.append(date_from)
            if date_from + timedelta(1) == date_to:
                break
            else:
                date_from = date_from + timedelta(1)

        # Поиск новых услуг
        for report_name in ("GoogleReport", "PlatAgentReport"):
            self._settings_service.choose_db("Aquapark_Ulyanovsk")
            new_tariffs = await self._settings_service.get_new_tariff(report_name)
            if new_tariffs:
                error_message = f"Найдены нераспределенные тарифы в отчете {report_name}: {new_tariffs}"
                logger.error(error_message)
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_message,
                )

        for date in period:
            date_from = date
            date_to = date + timedelta(1)
            await self.load_report(date_from, date_to, companies)

            self.orgs_dict = (
                await self._report_config_service.get_report_elements_with_groups(
                    "GoogleReport"
                )
            )
            self.itogreport_group_dict = (
                await self._report_config_service.get_report_elements_with_groups(
                    "ItogReport"
                )
            )
            self.agent_dict = (
                await self._report_config_service.get_report_elements_with_groups(
                    "PlatAgentReport"
                )
            )
            await self.save_reports(date_from, aqua_company=companies[0])

        # Отправка в яндекс диск
        if use_yadisk:
            self.path_list = filter(lambda x: x is not None, self.path_list)
            self._yandex_repo.sync_to_yadisk(
                self.path_list, settings.yadisk_token, date_from
            )
            self.path_list = []


def get_legacy_service() -> BarsicReport2Service:
    bars_srv = MsSqlDatabase(
        server=settings.mssql_server,
        user=settings.mssql_user,
        password=settings.mssql_pwd,
    )
    rk_srv = MsSqlDatabase(
        server=settings.mssql_server_rk,
        user=settings.mssql_user_rk,
        password=settings.mssql_pwd_rk,
    )
    return BarsicReport2Service(
        bars_srv=bars_srv,
        rk_srv=rk_srv,
    )
