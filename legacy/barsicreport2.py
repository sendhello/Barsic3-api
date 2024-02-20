import csv
import logging
import os
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List

import apiclient
import httplib2
import pyodbc
import yadisk
from dateutil.relativedelta import relativedelta
from lxml import etree, objectify
from oauth2client.service_account import ServiceAccountCredentials
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Protection, Side

from core.settings import settings
from db.mssql import get_mssql_connection
from legacy import functions, to_google_sheets


logger = logging.getLogger("barsicreport2")


class BarsicReport2Service:
    """
    Функционал предыдущей версии.
    """

    def __init__(self):
        self.org1 = None
        self.org2 = None
        self.org3 = None
        self.org4 = None
        self.org5 = None

        self.count_sql_error = 0
        self.org_for_finreport = {}
        self.new_service = []
        self.orgs = []
        self.new_agentservice = []
        self.agentorgs = []

        self.server = settings.mssql_server
        self.user = settings.mssql_user
        self.pwd = settings.mssql_pwd
        self.database1 = settings.mssql_database1
        self.database2 = settings.mssql_database2
        self.database_bitrix = settings.mssql_database_bitrix
        self.server_rk = settings.mssql_server_rk
        self.user_rk = settings.mssql_user_rk
        self.pwd_rk = settings.mssql_pwd_rk
        self.database_rk = settings.mssql_database_rk
        self.reportXML = settings.reportXML
        self.agentXML = settings.agentXML
        self.itogreportXML = settings.itogreportXML
        self.local_folder = settings.local_folder
        self.report_path = settings.report_path
        self.CREDENTIALS_FILE = settings.credentials_file
        self.list_google_docs = settings.list_google_docs
        self.yadisk_token = settings.yadisk_token
        self.google_all_read = settings.google_all_read
        self.google_reader_list = settings.google_reader_list
        self.google_writer_list = settings.google_writer_list

    def count_clients(
        self,
        server,
        database,
        uid,
        pwd,
    ):
        """Количество человек в зоне"""

        logging.info(
            f'{__name__}: {str(datetime.now())[:-7]}:    Выполнение функции "count_clients"'
        )

        result = []

        try:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Попытка соединения с {server}"
            )
            cnxn = get_mssql_connection(
                server=server, database=database, uid=uid, pwd=pwd
            )
            cursor = cnxn.cursor()

            cursor.execute(
                """
                    SELECT
                        [gr].[c1] as [c11],
                        [gr].[StockCategory_Id] as [StockCategory_Id1],
                        [c].[Name],
                        [c].[NN]
                    FROM
                        (
                            SELECT
                                [_].[CategoryId] as [StockCategory_Id],
                                Count(*) as [c1]
                            FROM
                                [AccountStock] [_]
                                    INNER JOIN [SuperAccount] [t1] ON [_].[SuperAccountId] = [t1].[SuperAccountId]
                            WHERE
                                [_].[StockType] = 41 AND
                                [t1].[Type] = 0 AND
                                [_].[Amount] > 0 AND
                                NOT ([t1].[IsStuff] = 1)
                            GROUP BY
                                [_].[CategoryId]
                        ) [gr]
                            INNER JOIN [Category] [c] ON [gr].[StockCategory_Id] = [c].[CategoryId]
               """
            )
            while True:
                row = cursor.fetchone()
                if row:
                    result.append(row)
                else:
                    break
            logging.info(
                f'{__name__}: {str(datetime.now())[:-7]}:    Результат функции "count_clients": {result}'
            )
            if not result:
                result.append(("Пусто", 488, "", "0003"))

        except pyodbc.OperationalError as e:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка {repr(e)}"
            )
            result.append(("Нет данных", 488, "Ошибка соединения", repr(e)))
            logger.info(f"Ошибка соединения с {server}: {database}", repr(e))
        except pyodbc.ProgrammingError as e:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка {repr(e)}"
            )
            result.append(("Нет данных", 488, "Ошибка соединения", repr(e)))
            logger.info(f"Невозможно открыть {database}", repr(e))
        return result

    def count_clients_print(self):
        in_zone = self.count_clients(
            server=self.server,
            database=self.database1,
            uid=self.user,
            pwd=self.pwd,
        )
        self.click_select_org()
        try:
            count_clients = int(
                self.itog_report(
                    server=self.server,
                    database=self.database1,
                    user=self.user,
                    pwd=self.pwd,
                    org=self.org1[0],
                    org_name=self.org1[1],
                    date_from=datetime.now(),
                    date_to=datetime.now() + timedelta(1),
                )["Аквазона"][0]
            )
        except KeyError:
            count_clients = 0
        try:
            count_clients_allday = self.reportClientCountTotals(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org1[0],
                date_from=datetime.now(),
                date_to=datetime.now() + timedelta(1),
            )[0][1]
        except IndexError:
            count_clients_allday = 0

        return {
            "Всего": str(count_clients) + " / " + str(count_clients_allday),
            str(in_zone[len(in_zone) - 1][2]): str(in_zone[len(in_zone) - 1][0]),
        }

    def click_select_org(self):
        """
        Выбор первой организации из списка организаций
        """
        org_list1 = self.list_organisation(
            server=self.server,
            database=self.database1,
            uid=self.user,
            pwd=self.pwd,
        )
        org_list2 = self.list_organisation(
            server=self.server,
            database=self.database2,
            uid=self.user,
            pwd=self.pwd,
        )
        for org in org_list1:
            if org[0] == 36:
                self.org1 = (org[0], org[2])
            if org[0] == 7203673:
                self.org3 = (org[0], org[2])
            if org[0] == 7203674:
                self.org4 = (org[0], org[2])
            if org[0] == 13240081:
                self.org5 = (org[0], org[2])

        logging.warning(f"{org_list1=}")
        logging.warning(f"{org_list2=}")
        self.org2 = (org_list2[0][0], org_list2[0][2])
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Выбраны организации {org_list1[0][2]} и {org_list2[0][2]}"
        )

    def list_organisation(
        self,
        server,
        database,
        uid,
        pwd,
    ):
        """Функция делает запрос в базу Барс и возвращает список заведенных в базе организаций в виде списка кортежей

        :param server: str - Путь до MS-SQL сервера, например 'SQLEXPRESS\BASE'
        :param database: str - Имя базы данных Барса, например 'SkiBars2'
        :param uid: str - Имя пользователя базы данных, например 'sa'
        :param pwd: str - Пароль пользователя базы данных, например 'pass'
        :return: list = Список организаций, каджая из которых - кортеж с параметрами организации
        """
        logging.info(f"{__name__}: {str(datetime.now())[:-7]}:    Поиск организаций...")
        result = []
        try:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Попытка соединения с {server}"
            )
            cnxn = get_mssql_connection(
                server=server, database=database, uid=uid, pwd=pwd
            )
            cursor = cnxn.cursor()

            id_type = 1
            cursor.execute(
                f"""
                SELECT
                    SuperAccountId, Type, Descr, CanRegister, CanPass, IsStuff, IsBlocked, BlockReason, DenyReturn, 
                    ClientCategoryId, DiscountCard, PersonalInfoId, Address, Inn, ExternalId, RegisterTime,LastTransactionTime, 
                    LegalEntityRelationTypeId, SellServicePointId, DepositServicePointId, AllowIgnoreStoredPledge, Email, 
                    Latitude, Longitude, Phone, WebSite, TNG_ProfileId
                FROM
                    SuperAccount
                WHERE
                    Type={id_type}
                """
            )
            while True:
                row = cursor.fetchone()
                if row:
                    result.append(row)
                else:
                    break
        except pyodbc.OperationalError as e:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка {repr(e)}"
            )
            logger.info(f"Ошибка соединения с {server}: {database}", repr(e))
            raise e
        except pyodbc.ProgrammingError as e:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка {repr(e)}"
            )
            logger.info(f"Невозможно открыть {database}", repr(e))
            raise e
        return result

    @functions.to_googleshet
    @functions.add_date
    @functions.add_sum
    @functions.convert_to_dict
    def itog_report(
        self,
        server,
        database,
        user,
        pwd,
        org,
        org_name,
        date_from,
        date_to,
        hide_zeroes="0",
        hide_internal="1",
        hide_discount="0",
    ):
        """Делает запрос в базу Барс и возвращает итоговый отчет за запрашиваемый период

        :param server: str - Путь до MS-SQL сервера, например 'SQLEXPRESS\BASE'
        :param database: str - Имя базы данных Барса, например 'SkiBars2'
        :param uid: str - Имя пользователя базы данных, например 'sa'
        :param pwd: str - Пароль пользователя базы данных, например 'pass'
        :param sa: str - Id организации в Барсе
        :param date_from: str - Начало отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :param date_to:  str - Конец отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :param hide_zeroes: 0 or 1 - Скрывать нулевые позиции?
        :param hide_internal: 0 or 1 - Скрывать внутренние точки обслуживания?
        :param hide_discount: 0 or 1 - Скрывать бонусы?
        :return: Итоговый отчет
        """
        cnxn = get_mssql_connection(server=server, database=database, uid=user, pwd=pwd)
        date_from = date_from.strftime("%Y%m%d 00:00:00")
        date_to = date_to.strftime("%Y%m%d 00:00:00")
        cursor = cnxn.cursor()
        SQL_REQUEST = (
            f"exec sp_reportOrganizationTotals_v2 "
            f"@sa={org},"
            f"@from='{date_from}',"
            f"@to='{date_to}',"
            f"@hideZeroes={hide_zeroes},"
            f"@hideInternal={hide_internal}"
        )
        # В аквапарке новая версия БД, добавляем новое поле в запрос
        if database == "Aquapark_Ulyanovsk":
            SQL_REQUEST += f",@hideDiscount={hide_discount}"

        cursor.execute(SQL_REQUEST)
        report = []
        while True:
            row = cursor.fetchone()
            if row:
                report.append(row)
            else:
                break
        report.append((0, 0, 0, 0, org_name, 0, "Организация", "Организация"))
        report.append((0, 0, 0, 0, str(org), 0, "ID организации", "ID организации"))
        if len(report) > 1:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Итоговый отчет сформирован ID организации = {org}, "
                f"Период: {date_from[:8]}-{date_to[:8]}, Скрывать нули = {hide_zeroes}, "
                f"Скрывать внутренние точки обслуживания: {hide_internal}, Cкрывать бонусы: {hide_discount})"
            )
        return report

    def reportClientCountTotals(
        self,
        server,
        database,
        user,
        pwd,
        org,
        date_from,
        date_to,
    ):
        cnxn = get_mssql_connection(server=server, database=database, uid=user, pwd=pwd)
        date_from = date_from.strftime("%Y%m%d 00:00:00")
        date_to = date_to.strftime("%Y%m%d 00:00:00")
        cursor = cnxn.cursor()
        cursor.execute(
            f"exec sp_reportClientCountTotals @sa={org},@from='{date_from}',@to='{date_to}',@categoryId=0"
        )
        report = []
        while True:
            row = cursor.fetchone()
            if row:
                report.append(row)
            else:
                break
        if len(report) > 1:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Отчет по количеству посетителей сформирован "
                f"ID организации = {org}, Период: {date_from[:8]}-{date_to[:8]}"
            )
        return report

    def client_count_totals_period(
        self,
        server,
        database,
        user,
        pwd,
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
                server=server,
                database=database,
                user=user,
                pwd=pwd,
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
        server,
        database,
        user,
        pwd,
        date_from,
        date_to,
    ):
        """Делает запрос в базу Барс и возвращает суммовой отчет за запрашиваемый период

        :param server: str - Путь до MS-SQL сервера, например 'SQLEXPRESS\BASE'
        :param database: str - Имя базы данных Барса, например 'SkiBars2'
        :param uid: str - Имя пользователя базы данных, например 'sa'
        :param pwd: str - Пароль пользователя базы данных, например 'pass'
        :param sa: str - Id организации в Барсе
        :param date_from: str - Начало отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :param date_to:  str - Конец отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :return: Суммовой отчет
        """
        cnxn = get_mssql_connection(server=server, database=database, uid=user, pwd=pwd)
        date_from = date_from.strftime("%Y%m%d 00:00:00")
        date_to = date_to.strftime("%Y%m%d 00:00:00")
        cursor = cnxn.cursor()
        cursor.execute(
            f"exec sp_reportCashDeskMoney @from='{date_from}', @to='{date_to}'"
        )
        report = []
        while True:
            row = cursor.fetchone()
            if row:
                report.append(row)
            else:
                break
        if len(report) > 1:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Суммовой отчет сформирован, "
                f"Период: {date_from[:8]}-{date_to[:8]}"
            )
        return report

    def service_point_request(
        self,
        server,
        database,
        user,
        pwd,
    ):
        """Делает запрос в базу Барс и возвращает список рабочих мест

        :param server: str - Путь до MS-SQL сервера, например 'SQLEXPRESS\BASE'
        :param database: str - Имя базы данных Барса, например 'SkiBars2'
        :param uid: str - Имя пользователя базы данных, например 'sa'
        :param pwd: str - Пароль пользователя базы данных, например 'pass'
        :param sa: str - Id организации в Барсе
        :return: Суммовой отчет
        """
        cnxn = get_mssql_connection(server=server, database=database, uid=user, pwd=pwd)
        cursor = cnxn.cursor()
        cursor.execute(
            f"""
            SELECT
                ServicePointId, Name, SuperAccountId, Type, Code, IsInternal
            FROM 
                ServicePoint
            """
        )
        report = []
        while True:
            row = cursor.fetchone()
            if row:
                report.append(row)
            else:
                break
        if len(report) > 1:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Список рабочих мест сформирован."
            )
        return report

    def cashdesk_report(
        self,
        server,
        database,
        user,
        pwd,
        date_from,
        date_to,
    ):
        """
        Преобразует запросы из базы в суммовой отчет
        :return: dict
        """
        cash_report = self.cash_report_request(
            server=server,
            database=database,
            user=user,
            pwd=pwd,
            date_from=date_from,
            date_to=date_to,
        )
        service_point = self.service_point_request(
            server=server,
            database=database,
            user=user,
            pwd=pwd,
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
        if database == self.database1:
            report["Организация"] = [[self.org1[1]]]
        elif database == self.database2:
            report["Организация"] = [[self.org2[1]]]
        return report

    def read_bitrix_base(
        self,
        server,
        database,
        user,
        pwd,
        date_from,
        date_to,
    ):
        """Функция делает запрос в базу и возвращает список продаж за указанную дату

        :param server: str - Путь до MS-SQL сервера, например 'SQLEXPRESS\BASE'
        :param database: str - Имя базы данных Барса, например 'SkiBars2'
        :param uid: str - Имя пользователя базы данных, например 'sa'
        :param pwd: str - Пароль пользователя базы данных, например 'pass'
        :param date_from: str - Начало отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :param date_to:  str - Конец отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :return: list = Список организаций, каджая из которых - кортеж с параметрами организации
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Чтение online-продаж..."
        )
        date_from = date_from.strftime("%Y%m%d") + " 00:00:00"
        date_to = date_to.strftime("%Y%m%d") + " 00:00:00"

        cnxn = get_mssql_connection(server=server, database=database, uid=user, pwd=pwd)
        cursor = cnxn.cursor()
        cursor.execute(
            f"""
            SELECT 
                Id, OrderNumber, ProductId, ProductName, OrderDate, PayDate, Sum, Pay, Status, Client
            FROM 
                Transactions
            WHERE
                (PayDate >= '{date_from}')and(PayDate < '{date_to}')
            """
        )
        orders = []
        while True:
            row = cursor.fetchone()
            if row:
                orders.append(row)
            else:
                break
        summ = 0
        for order in orders:
            summ += float(order[6])
        return len(orders), summ

    def read_reportgroup(self, XML):
        """
        Чтение XML с привязкой групп услуг к услугам
        :param path:
        :return:
        """
        with open(XML, encoding="utf-8") as f:
            xml = f.read()

        root = objectify.fromstring(xml)
        orgs_dict = {}

        for org in root.UrFace:
            orgs_dict[org.get("Name")] = []
            for serv in org.Services.Service:
                if serv.get("Name") != "Пустая обязательная категория":
                    orgs_dict[org.get("Name")].append(serv.get("Name"))

        return orgs_dict

    @staticmethod
    def rk_report_request(
        server: str,
        database: str,
        user: str,
        pwd: str,
        cash_id: int,
        date_from: datetime,
        date_to: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Делает запрос в базу R-Keeper и возвращает продажи кассы cash_id за запрашиваемый период
        :param server: str - Путь до MS-SQL сервера, например 'SQLEXPRESS\BASE'
        :param database: str - Имя базы данных R-Keeper, например 'RK7'
        :param user: str - Имя пользователя базы данных, например 'sa'
        :param pwd: str - Пароль пользователя базы данных, например 'pass'
        :param cash_id: int - Id кассы R-Keeper
        :param date_from: datetime - Начало отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :param date_to:  datetime - Конец отчетного периода в формате: 'YYYYMMDD 00:00:00'
        :return: {
            'station_id': int,
            'open_time': datetime(...),
            'paid_sum': Decimal()
        }
        """
        cnxn = get_mssql_connection(server=server, database=database, uid=user, pwd=pwd)
        date_from = date_from.strftime("%Y%m%d 00:00:00")
        date_to = date_to.strftime("%Y%m%d 00:00:00")
        cursor = cnxn.cursor()
        cursor.execute(
            f"""{''}
                SELECT OPENTIME, STATIONID, PAIDSUM FROM ORDERS 
                WHERE STATIONID = {cash_id} AND OPENTIME > '{date_from}' AND OPENTIME < '{date_to}'
            """
        )
        rows = cursor.fetchall()
        report = [
            {"station_id": row[0], "open_time": row[1], "paid_sum": row[2]}
            for row in rows
        ]
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    "
            f"Отчет по P-Keeper сфомирован, Период: {date_from[:8]}-{date_to[:8]}"
        )
        return report

    def find_new_service(self, service_dict, orgs_dict):
        """
        Поиск новых услуг и организаций из XML
        :param service_dict: Итоговый отчет
        :param orgs_dict: словарь из XML-файла
        :return:
        """
        servise_set = set()

        for key in orgs_dict:
            for s in orgs_dict[key]:
                servise_set.add(s)

        for org in service_dict:
            if org not in servise_set and org not in self.new_service:
                self.new_service.append(org)
                servise_set.add(org)

        for key in orgs_dict:
            if key not in self.orgs:
                self.orgs.append(key)

    def create_new_org(self, name, service):
        """
        Добавляет новую организацию в список организаций self.orgs, словарь self.orgs_dict и XML конфигурацию.
        Возвращает изьятую ранее услугу в список новых услуг с помощью функции self.readd_org
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Добавление новой группы - {name}"
        )
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Добавление услуги {service} в группу {name}"
        )
        self.orgs.append(name)
        self.orgs_dict[name] = []
        self.readd_org(service)
        with open(self.reportXML, encoding="utf-8") as f:
            xml = f.read()
        root = objectify.fromstring(xml)
        # Добавляем новые организации
        new_org = objectify.SubElement(root, "UrFace")
        new_org.set("Name", name)
        new_servs = objectify.SubElement(new_org, "Services")
        new_serv = objectify.SubElement(new_servs, "Service")
        new_serv.set("Name", "Пустая обязательная категория")
        # удаляем аннотации.
        objectify.deannotate(root)
        etree.cleanup_namespaces(root)
        obj_xml = etree.tostring(
            root, encoding="utf-8", pretty_print=True, xml_declaration=True
        )
        # сохраняем данные в файл.
        try:
            with open(self.reportXML, "w", encoding="utf_8_sig") as xml_writer:
                xml_writer.write(obj_xml.decode("utf-8"))
        except IOError:
            pass

    def agentservice(self, date_from):
        self.agent_dict = self.read_reportgroup(self.agentXML)
        self.find_new_agentservice(self.itog_report_org1, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org1_lastyear, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org3, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org3_lastyear, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org4, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org4_lastyear, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org5, self.agent_dict)
        self.find_new_agentservice(self.itog_report_org5_lastyear, self.agent_dict)
        if self.itog_report_month:
            self.find_new_agentservice(self.itog_report_month, self.agent_dict)

        if self.new_agentservice:
            logger.warning(f"Найдены новые сервисы: {self.new_agentservice}")

        self.save_reports(date_from)

    def find_new_agentservice(self, service_dict, orgs_dict):
        """
        Поиск новых услуг и организаций из XML
        :param service_dict: Итоговый отчет
        :param orgs_dict: словарь из XML-файла
        :return:
        """
        servise_set = set()

        for key in orgs_dict:
            for s in orgs_dict[key]:
                servise_set.add(s)

        for org in service_dict:
            if org not in servise_set and org not in self.new_agentservice:
                self.new_agentservice.append(org)
                servise_set.add(org)

        for key in orgs_dict:
            if key not in self.agentorgs:
                self.agentorgs.append(key)

    def create_new_agentorg(self, name, service):
        """
        Добавляет новую организацию в список организаций self.orgs, словарь self.orgs_dict и XML конфигурацию.
        Возвращает изьятую ранее услугу в список новых услуг с помощью функции self.readd_org
        """
        self.agentorgs.append(name)
        self.agent_dict[name] = []
        self.readd_agentorg(service)
        with open(self.agentXML, encoding="utf-8") as f:
            xml = f.read()
        root = objectify.fromstring(xml)
        # Добавляем новые организации
        new_org = objectify.SubElement(root, "UrFace")
        new_org.set("Name", name)
        new_servs = objectify.SubElement(new_org, "Services")
        new_serv = objectify.SubElement(new_servs, "Service")
        new_serv.set("Name", "Пустая обязательная категория")
        # удаляем аннотации.
        objectify.deannotate(root)
        etree.cleanup_namespaces(root)
        obj_xml = etree.tostring(
            root, encoding="utf-8", pretty_print=True, xml_declaration=True
        )
        # сохраняем данные в файл.
        try:
            with open(self.agentXML, "w", encoding="utf_8_sig") as xml_writer:
                xml_writer.write(obj_xml.decode("utf-8"))
        except IOError:
            pass

    def fin_report(self):
        """
        Форминует финансовый отчет в установленном формате
        :return - dict
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Формирование финансового отчета"
        )
        self.finreport_dict = {}
        is_aquazona = None
        for org, services in self.orgs_dict.items():
            if org != "Не учитывать":
                self.finreport_dict[org] = [0, 0.00]
                for serv in services:
                    try:
                        if org == "Дата":
                            self.finreport_dict[org][0] = self.itog_report_org1[serv][0]
                            self.finreport_dict[org][1] = self.itog_report_org1[serv][1]
                        elif serv == "Депозит":
                            self.finreport_dict[org][1] += self.itog_report_org1[serv][
                                1
                            ]
                        elif serv == "Аквазона":
                            self.finreport_dict["Кол-во проходов"] = [
                                self.itog_report_org1[serv][0],
                                0,
                            ]
                            self.finreport_dict[org][1] += self.itog_report_org1[serv][
                                1
                            ]
                            is_aquazona = True
                        elif serv == "Организация":
                            pass
                        else:
                            if (
                                self.itog_report_org1.get(serv)
                                and self.itog_report_org1[serv][1] != 0.0
                            ):
                                self.finreport_dict[org][0] += self.itog_report_org1[
                                    serv
                                ][0]
                                self.finreport_dict[org][1] += self.itog_report_org1[
                                    serv
                                ][1]
                            if (
                                self.itog_report_org3.get(serv)
                                and self.itog_report_org3[serv][1] != 0.0
                            ):
                                self.finreport_dict[org][0] += self.itog_report_org3[
                                    serv
                                ][0]
                                self.finreport_dict[org][1] += self.itog_report_org3[
                                    serv
                                ][1]
                            if (
                                self.itog_report_org4.get(serv)
                                and self.itog_report_org4[serv][1] != 0.0
                            ):
                                self.finreport_dict[org][0] += self.itog_report_org4[
                                    serv
                                ][0]
                                self.finreport_dict[org][1] += self.itog_report_org4[
                                    serv
                                ][1]
                            if (
                                self.itog_report_org5.get(serv)
                                and self.itog_report_org5[serv][1] != 0.0
                            ):
                                self.finreport_dict[org][0] += self.itog_report_org5[
                                    serv
                                ][0]
                                self.finreport_dict[org][1] += self.itog_report_org5[
                                    serv
                                ][1]
                    except KeyError:
                        pass
                    except TypeError:
                        pass

        if not is_aquazona:
            self.finreport_dict["Кол-во проходов"] = [0, 0.00]

        self.finreport_dict.setdefault("Online Продажи", [0, 0.0])
        self.finreport_dict["Online Продажи"][0] += self.report_bitrix[0]
        self.finreport_dict["Online Продажи"][1] += self.report_bitrix[1]

        self.finreport_dict["Смайл"][0] = len(self.report_rk)
        self.finreport_dict["Смайл"][1] = float(
            sum([line["paid_sum"] for line in self.report_rk])
        )

        total_cashdesk_report = self.cashdesk_report_org1["Итого"][0]
        self.finreport_dict["MaxBonus"] = (
            0,
            float(total_cashdesk_report[6] - total_cashdesk_report[7]),
        )

    def fin_report_lastyear(self):
        """
        Форминует финансовый отчет за предыдущий год в установленном формате
        :return - dict
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Формирование финансового отчета за прошлый год"
        )
        self.finreport_dict_lastyear = {}
        is_aquazona = None
        for org, services in self.orgs_dict.items():
            if org != "Не учитывать":
                self.finreport_dict_lastyear[org] = [0, 0.00]
                for serv in services:
                    try:
                        if org == "Дата":
                            self.finreport_dict_lastyear[org][0] = (
                                self.itog_report_org1_lastyear[serv][0]
                            )
                            self.finreport_dict_lastyear[org][1] = (
                                self.itog_report_org1_lastyear[serv][1]
                            )
                        elif serv == "Депозит":
                            self.finreport_dict_lastyear[org][
                                1
                            ] += self.itog_report_org1_lastyear[serv][1]
                        elif serv == "Аквазона":
                            self.finreport_dict_lastyear["Кол-во проходов"] = [
                                self.itog_report_org1_lastyear[serv][0],
                                0,
                            ]
                            self.finreport_dict_lastyear[org][
                                1
                            ] += self.itog_report_org1_lastyear[serv][1]
                            is_aquazona = True
                        elif serv == "Организация":
                            pass
                        else:
                            if (
                                self.itog_report_org1_lastyear.get(serv)
                                and self.itog_report_org1_lastyear[serv][1] != 0.0
                            ):
                                self.finreport_dict_lastyear[org][
                                    0
                                ] += self.itog_report_org1_lastyear[serv][0]
                                self.finreport_dict_lastyear[org][
                                    1
                                ] += self.itog_report_org1_lastyear[serv][1]
                            if (
                                self.itog_report_org3_lastyear.get(serv)
                                and self.itog_report_org3_lastyear[serv][1] != 0.0
                            ):
                                self.finreport_dict_lastyear[org][
                                    0
                                ] += self.itog_report_org3_lastyear[serv][0]
                                self.finreport_dict_lastyear[org][
                                    1
                                ] += self.itog_report_org3_lastyear[serv][1]
                            if (
                                self.itog_report_org4_lastyear.get(serv)
                                and self.itog_report_org4_lastyear[serv][1] != 0.0
                            ):
                                self.finreport_dict_lastyear[org][
                                    0
                                ] += self.itog_report_org4_lastyear[serv][0]
                                self.finreport_dict_lastyear[org][
                                    1
                                ] += self.itog_report_org4_lastyear[serv][1]
                            if (
                                self.itog_report_org5_lastyear.get(serv)
                                and self.itog_report_org5_lastyear[serv][1] != 0.0
                            ):
                                self.finreport_dict_lastyear[org][
                                    0
                                ] += self.itog_report_org5_lastyear[serv][0]
                                self.finreport_dict_lastyear[org][
                                    1
                                ] += self.itog_report_org5_lastyear[serv][1]
                    except KeyError:
                        pass
                    except TypeError:
                        pass
        if not is_aquazona:
            self.finreport_dict_lastyear["Кол-во проходов"] = [0, 0.00]
        self.finreport_dict_lastyear.setdefault("Online Продажи", [0, 0.0])
        self.finreport_dict_lastyear["Online Продажи"][
            0
        ] += self.report_bitrix_lastyear[0]
        self.finreport_dict_lastyear["Online Продажи"][
            1
        ] += self.report_bitrix_lastyear[1]
        self.finreport_dict_lastyear["Смайл"][0] = len(self.report_rk_lastyear)
        self.finreport_dict_lastyear["Смайл"][1] = float(
            sum([line["paid_sum"] for line in self.report_rk_lastyear])
        )

        total_cashdesk_report = self.cashdesk_report_org1_lastyear["Итого"][0]
        self.finreport_dict_lastyear["MaxBonus"] = (
            0,
            total_cashdesk_report[6] - total_cashdesk_report[7],
        )

    def fin_report_month(self):
        """
        Форминует финансовый отчет за месяц в установленном формате
        :return - dict
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Формирование финансового отчета за месяц"
        )
        self.finreport_dict_month = {}
        control_sum_group = self.finreport_dict_month.setdefault(
            "Контрольная сумма", {}
        )
        control_sum = control_sum_group.setdefault("Cумма", [["Сумма", 0, 0.0]])
        smile = [
            len(self.report_rk_month),
            float(sum([line["paid_sum"] for line in self.report_rk_month])),
        ]

        for group_name, groups in self.itogreport_group_dict.items():
            finreport_group = self.finreport_dict_month.setdefault(group_name, {})
            finreport_group_total = finreport_group.setdefault(
                "Итого по группе", [["Итого по группе", 0, 0.0]]
            )
            for oldgroup in groups:
                try:
                    for product_name in self.orgs_dict[oldgroup]:
                        try:
                            product = self.itog_report_month[product_name]

                            if product_name == "Дата":
                                product_group = finreport_group.setdefault(oldgroup, [])
                                product_group.append(
                                    [product_name, product[0], product[1]]
                                )
                            elif product_name == "Депозит":
                                product_group = finreport_group.setdefault(oldgroup, [])
                                product_group.append([product_name, 0, product[1]])
                                finreport_group_total[0][2] += product[1]
                                control_sum[0][2] += product[1]
                            elif product_name == "Организация":
                                pass
                            else:
                                product_group = finreport_group.setdefault(
                                    product[2], [["Итого по папке", 0, 0.0]]
                                )
                                product_group.append(
                                    [product_name, product[0], product[1]]
                                )
                                product_group[0][1] += product[0]
                                product_group[0][2] += product[1]
                                finreport_group_total[0][1] += product[0]
                                finreport_group_total[0][2] += product[1]
                                if product_name != "Итого по отчету":
                                    control_sum[0][1] += product[0]
                                    control_sum[0][2] += product[1]
                        except KeyError:
                            continue
                        except TypeError:
                            continue

                except KeyError as e:
                    logger.info(
                        "Несоответствие конфигураций XML-файлов",
                        f"Группа {oldgroup} не существует!\n" f"KeyError: {e}",
                    )
                    logging.error(
                        f"{__name__}: {str(datetime.now())[:-7]}:    Несоответствие конфигураций XML-файлов\n"
                        f"Группа {oldgroup} не существует! \nKeyError: {e}"
                    )

                if oldgroup == "Общепит":
                    product_group = finreport_group.setdefault(
                        "Общепит ------------------------------------------------------------------------------ ИП Салахова",
                        [["Итого по папке", 0, 0.0]],
                    )
                    product_group.append(["Смайл", smile[0], smile[1]])
                    product_group[0][1] += smile[0]
                    product_group[0][2] += smile[1]
                    finreport_group_total[0][1] += smile[0]
                    finreport_group_total[0][2] += smile[1]

        control_sum[0][1] += smile[0]
        control_sum[0][2] += smile[1]
        self.finreport_dict_month["ИТОГО"]["Итого по группе"][0][1] += smile[0]
        self.finreport_dict_month["ИТОГО"]["Итого по группе"][0][2] += smile[1]
        self.finreport_dict_month["ИТОГО"][""][0][1] += smile[0]
        self.finreport_dict_month["ИТОГО"][""][0][2] += smile[1]
        self.finreport_dict_month["ИТОГО"][""][1][1] += smile[0]
        self.finreport_dict_month["ИТОГО"][""][1][2] += smile[1]
        if (
            self.finreport_dict_month["ИТОГО"][""][1][2] != control_sum[0][2]
            or self.finreport_dict_month["ИТОГО"][""][1][1] != control_sum[0][1]
        ):
            logger.info(
                "Несоответствие Контрольных сумм.",
                f"Итого по отчету ({self.finreport_dict_month['ИТОГО'][''][1][1]}: "
                f"{self.finreport_dict_month['ИТОГО'][''][1][2]}) не равно Контрольной сумме услуг"
                f"({control_sum[0][1]}: {control_sum[0][2]})",
            )
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Несоответствие Контрольных сумм. "
                f"Итого по отчету ({self.finreport_dict_month['ИТОГО'][''][1][1]}: "
                f"{self.finreport_dict_month['ИТОГО'][''][1][2]}) не равно Контрольной сумме услуг"
                f"({control_sum[0][1]}: {control_sum[0][2]})"
            )

    def fin_report_beach(self):
        """
        Форминует финансовый отчет по пляжу в установленном формате
        :return - dict
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Формирование финансового отчета по пляжу"
        )
        self.finreport_dict_beach = {
            "Депозит": (0, 0),
            "Товары": (0, 0),
            "Услуги": (0, 0),
            "Карты": (0, 0),
            "Итого по отчету": (0, 0),
        }
        for service in self.itog_report_org2:
            if service == "Дата":
                self.finreport_dict_beach[service] = (
                    self.itog_report_org2[service][0],
                    self.itog_report_org2[service][1],
                )
            elif service == "Выход с пляжа":
                self.finreport_dict_beach[service] = (
                    self.itog_report_org2[service][0],
                    self.itog_report_org2[service][1],
                )
            elif not self.itog_report_org2[service][3] in self.finreport_dict_beach:
                self.finreport_dict_beach[self.itog_report_org2[service][3]] = (
                    self.itog_report_org2[service][0],
                    self.itog_report_org2[service][1],
                )
            else:
                try:
                    self.finreport_dict_beach[self.itog_report_org2[service][3]] = (
                        self.finreport_dict_beach[self.itog_report_org2[service][3]][0]
                        + self.itog_report_org2[service][0],
                        self.finreport_dict_beach[self.itog_report_org2[service][3]][1]
                        + self.itog_report_org2[service][1],
                    )
                except TypeError:
                    pass
        if "Выход с пляжа" not in self.finreport_dict_beach:
            self.finreport_dict_beach["Выход с пляжа"] = 0, 0

    def agent_report(self):
        """
        Форминует отчет платежного агента в установленном формате
        :return - dict
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Формирование отчета платежного агента"
        )
        self.agentreport_dict = {}
        self.agentreport_dict["Организация"] = [self.org1[0], self.org1[1]]
        for org, services in self.agent_dict.items():
            if org != "Не учитывать":
                self.agentreport_dict[org] = [0, 0]
                for serv in services:
                    try:
                        if org == "Дата":
                            self.agentreport_dict[org][0] = self.itog_report_org1[serv][
                                0
                            ]
                            self.agentreport_dict[org][1] = self.itog_report_org1[serv][
                                1
                            ]
                        elif serv == "Депозит":
                            self.agentreport_dict[org][1] += self.itog_report_org1[
                                serv
                            ][1]
                        elif serv == "Аквазона":
                            self.agentreport_dict[org][1] += self.itog_report_org1[
                                serv
                            ][1]
                        elif serv == "Организация":
                            pass
                        else:
                            self.agentreport_dict[org][0] += self.itog_report_org1[
                                serv
                            ][0]
                            self.agentreport_dict[org][1] += self.itog_report_org1[
                                serv
                            ][1]
                    except KeyError:
                        pass
                    except TypeError:
                        pass

    def agent_report_month(self):
        """
        Форминует отчет платежного агента за месяц в установленном формате
        :return - dict
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Формирование отчета платежного агента за месяц"
        )
        self.agentreport_dict_month = {}
        self.agentreport_dict_month["Контрольная сумма"] = {}
        self.agentreport_dict_month["Контрольная сумма"]["Cумма"] = [["Сумма", 0, 0.0]]
        for org in self.agent_dict:
            self.agentreport_dict_month[org] = {}
            self.agentreport_dict_month[org]["Итого по группе"] = [
                ["Итого по группе", 0, 0.0]
            ]
            for tariff in self.agent_dict[org]:
                try:
                    if tariff == "Дата":
                        self.agentreport_dict_month[org][tariff] = []
                        self.agentreport_dict_month[org][tariff].append(
                            [
                                tariff,
                                self.itog_report_month[tariff][0],
                                self.itog_report_month[tariff][1],
                            ]
                        )
                    elif tariff == "Депозит":
                        self.agentreport_dict_month[org][tariff] = []
                        self.agentreport_dict_month[org][tariff].append(
                            [tariff, 0, self.itog_report_month[tariff][1]]
                        )
                        self.agentreport_dict_month[org]["Итого по группе"][0][
                            2
                        ] += self.itog_report_month[tariff][1]
                        self.agentreport_dict_month["Контрольная сумма"]["Cумма"][0][
                            2
                        ] += self.itog_report_month[tariff][1]
                    elif tariff == "Организация":
                        pass
                    else:
                        try:
                            if self.agentreport_dict_month[org][
                                self.itog_report_month[tariff][2]
                            ]:
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ].append(
                                    [
                                        tariff,
                                        self.itog_report_month[tariff][0],
                                        self.itog_report_month[tariff][1],
                                    ]
                                )
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ][0][1] += self.itog_report_month[tariff][0]
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ][0][2] += self.itog_report_month[tariff][1]
                                self.agentreport_dict_month[org]["Итого по группе"][0][
                                    1
                                ] += self.itog_report_month[tariff][0]
                                self.agentreport_dict_month[org]["Итого по группе"][0][
                                    2
                                ] += self.itog_report_month[tariff][1]
                                if tariff != "Итого по отчету":
                                    self.agentreport_dict_month["Контрольная сумма"][
                                        "Cумма"
                                    ][0][1] += self.itog_report_month[tariff][0]
                                    self.agentreport_dict_month["Контрольная сумма"][
                                        "Cумма"
                                    ][0][2] += self.itog_report_month[tariff][1]
                            else:
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ] = []
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ].append(["Итого по папке", 0, 0.0])
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ].append(
                                    [
                                        tariff,
                                        self.itog_report_month[tariff][0],
                                        self.itog_report_month[tariff][1],
                                    ]
                                )
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ][0][1] += self.itog_report_month[tariff][0]
                                self.agentreport_dict_month[org][
                                    self.itog_report_month[tariff][2]
                                ][0][2] += self.itog_report_month[tariff][1]
                                self.agentreport_dict_month[org]["Итого по группе"][0][
                                    1
                                ] += self.itog_report_month[tariff][0]
                                self.agentreport_dict_month[org]["Итого по группе"][0][
                                    2
                                ] += self.itog_report_month[tariff][1]
                                if tariff != "Итого по отчету":
                                    self.agentreport_dict_month["Контрольная сумма"][
                                        "Cумма"
                                    ][0][1] += self.itog_report_month[tariff][0]
                                    self.agentreport_dict_month["Контрольная сумма"][
                                        "Cумма"
                                    ][0][2] += self.itog_report_month[tariff][1]
                        except KeyError:
                            self.agentreport_dict_month[org][
                                self.itog_report_month[tariff][2]
                            ] = []
                            self.agentreport_dict_month[org][
                                self.itog_report_month[tariff][2]
                            ].append(["Итого по папке", 0, 0.0])
                            self.agentreport_dict_month[org][
                                self.itog_report_month[tariff][2]
                            ].append(
                                (
                                    tariff,
                                    self.itog_report_month[tariff][0],
                                    self.itog_report_month[tariff][1],
                                )
                            )
                            self.agentreport_dict_month[org][
                                self.itog_report_month[tariff][2]
                            ][0][1] += self.itog_report_month[tariff][0]
                            self.agentreport_dict_month[org][
                                self.itog_report_month[tariff][2]
                            ][0][2] += self.itog_report_month[tariff][1]
                            self.agentreport_dict_month[org]["Итого по группе"][0][
                                1
                            ] += self.itog_report_month[tariff][0]
                            self.agentreport_dict_month[org]["Итого по группе"][0][
                                2
                            ] += self.itog_report_month[tariff][1]
                            if tariff != "Итого по отчету":
                                self.agentreport_dict_month["Контрольная сумма"][
                                    "Cумма"
                                ][0][1] += self.itog_report_month[tariff][0]
                                self.agentreport_dict_month["Контрольная сумма"][
                                    "Cумма"
                                ][0][2] += self.itog_report_month[tariff][1]
                except KeyError:
                    pass
                except TypeError:
                    pass
        if (
            self.agentreport_dict_month["ИТОГО"][""][1][2]
            != self.agentreport_dict_month["Контрольная сумма"]["Cумма"][0][2]
            or self.agentreport_dict_month["ИТОГО"][""][1][1]
            != self.agentreport_dict_month["Контрольная сумма"]["Cумма"][0][1]
        ):
            logger.info(
                "Несоответствие Контрольных сумм.",
                f"Итого по отчету ({self.agentreport_dict_month['ИТОГО'][''][1][1]}: "
                f"{self.agentreport_dict_month['ИТОГО'][''][1][2]}) не равно Контрольной сумме услуг"
                f"({self.agentreport_dict_month['Контрольная сумма']['Cумма'][0][1]}: "
                f"{self.agentreport_dict_month['Контрольная сумма']['Cумма'][0][2]})",
            )
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Несоответствие Контрольных сумм. "
                f"Итого по отчету ({self.agentreport_dict_month['ИТОГО'][''][1][1]}: "
                f"{self.agentreport_dict_month['ИТОГО'][''][1][2]}) не равно Контрольной сумме услуг"
                f"({self.agentreport_dict_month['Контрольная сумма']['Cумма'][0][1]}: "
                f"{self.agentreport_dict_month['Контрольная сумма']['Cумма'][0][2]})"
            )

    def export_agent_report(self, agentreport_dict, date_from):
        """
        Сохраняет отчет платежного агента в виде Excel-файла в локальную директорию
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
        fill = PatternFill(fill_type="solid", start_color="c1c1c1", end_color="c2c2c2")
        table_color = PatternFill(
            fill_type="solid", start_color="e2e2e2", end_color="e9e9e9"
        )
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
        number_format = "General"
        protection = Protection(locked=True, hidden=False)

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
        ws["A1"].font = h1
        # выравнивание
        ws["A1"].alignment = align_left

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
        ws[column[1] + self.row].font = h1
        # выравнивание
        ws[column[1] + self.row].alignment = align_left
        # Высота строк
        ws.row_dimensions[1].height = 24

        ws[column[1] + next_row()] = f'{agentreport_dict["Организация"][1]}'
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        ws[column[1] + self.row].font = font
        ws[column[1] + self.row].alignment = align_top

        ws[column[1] + next_row()] = "За период с:"
        ws[column[1] + self.row].font = font
        ws[column[1] + self.row].alignment = align_top
        ws[column[2] + self.row] = (agentreport_dict["Дата"][0]).strftime("%d.%m.%Y")
        ws[column[2] + self.row].font = font_bold
        ws[column[2] + self.row].alignment = align_top
        ws[column[3] + self.row] = "по"
        ws[column[3] + self.row].font = font
        ws[column[3] + self.row].alignment = align_top
        ws[column[4] + self.row] = (
            agentreport_dict["Дата"][1] - timedelta(1)
        ).strftime("%d.%m.%Y")
        ws[column[4] + self.row].font = font_bold
        ws[column[4] + self.row].alignment = align_top

        # ТАБЛИЦА
        self.color = False

        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=4
            )
            ws[column[1] + self.row].font = font
            ws[column[5] + self.row].font = font
            ws[column[1] + self.row].alignment = align_top
            ws[column[5] + self.row].alignment = align_top
            ws[column[5] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
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
            ws[column[1] + self.row].font = font_bold
            ws[column[5] + self.row].font = font_bold
            ws[column[1] + self.row].alignment = align_top
            ws[column[5] + self.row].alignment = align_top
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
                b += 1

        ws[column[1] + next_row()] = "Наименование поставщика услуг"
        ws[column[5] + self.row] = "Сумма"
        merge_table_bold()
        # раскрашивание фона для заголовков
        b = 1
        while b < len(column):
            ws[column[b] + self.row].fill = fill
            b += 1

        itog_sum = 0
        for line in agentreport_dict:
            if line != "Организация" and line != "Дата" and line != "ИТОГО":
                try:
                    itog_sum += agentreport_dict[line][1]
                    ws[column[1] + next_row()] = line
                    ws[column[5] + self.row] = agentreport_dict[line][1]
                    merge_table()
                except AttributeError:
                    pass

        ws[column[1] + next_row()] = "Итого"
        if itog_sum != agentreport_dict["ИТОГО"][1]:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка. Отчет платежного агента: сумма строк "
                f"({itog_sum}) не равна строке ИТОГО "
                f'({agentreport_dict["ИТОГО"][1]})'
            )
            logger.info(
                f"Ошибка. Отчет платежного агента",
                f"Ошибка. Отчет платежного агента: сумма строк "
                f"({itog_sum}) не равна строке ИТОГО "
                f'({agentreport_dict["ИТОГО"][1]})',
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
        if agentreport_dict["Дата"][0] == agentreport_dict["Дата"][1] - timedelta(1):
            date_ = datetime.strftime(agentreport_dict["Дата"][0], "%Y-%m-%d")
        else:
            date_ = (
                f'{datetime.strftime(agentreport_dict["Дата"][0], "%Y-%m-%d")} - '
                f'{datetime.strftime(agentreport_dict["Дата"][1], "%Y-%m-%d")}'
            )
        path = (
            self.local_folder
            + self.report_path
            + date_
            + f' Отчет платежного агента {agentreport_dict["Организация"][1]}'
            + ".xlsx"
        )
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Сохранение отчета платежного агента "
            f'{agentreport_dict["Организация"][1]} в {path}'
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def create_path(self, path, date_from):
        """
        Проверяет наличие указанного пути. В случае отсутствия каких-либо папок создает их
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Проверка локальных путей сохранения файлов..."
        )
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
                logging.warning(
                    f'{__name__}: {str(datetime.now())[:-7]}:    В директории "{os.getcwd()}" создана папка "{folder}"'
                )
                os.chdir(folder)
            else:
                os.chdir(folder)
            path += folder + "/"
        path += end_path
        os.chdir(directory)
        return path

    def save_file(self, path, file):
        """
        Проверяет не занят ли файл другим процессом и если нет, то перезаписывает его, в противном
        случае выводит диалоговое окно с предложением закрыть файл и продолжить
        """
        try:
            file.save(path)
        except PermissionError as e:
            logging.error(
                f'{__name__}: {str(datetime.now())[:-7]}:    Файл "{path}" занят другим процессом.\n{repr(e)}'
            )
            logger.info(
                f"Ошибка записи файла",
                f'Файл "{path}" занят другим процессом.\nДля повтора попытки закройте это сообщение',
                func=self.save_file,
                path=path,
                file=file,
            )

    def sync_to_yadisk(self, path_list, token, date_from):
        """
        Копирует локальные файлы в Яндекс Диск
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Копирование отчетов в Яндекс.Диск..."
        )
        if path_list:
            if self.use_yadisk:
                logging.info(
                    f"{__name__}: {str(datetime.now())[:-7]}:    Соединение с YaDisk..."
                )
                self.yadisk = yadisk.YaDisk(token=token)
                if self.yadisk.check_token():
                    path = "" + self.report_path
                    remote_folder = self.create_path_yadisk(path, date_from)
                    for local_path in path_list:
                        remote_path = remote_folder + local_path.split("/")[-1]
                        logging.info(
                            f'{__name__}: {str(datetime.now())[:-7]}:    Отправка файла "{local_path.split("/")[-1]}" в YaDisk...'
                        )
                        files_list_yandex = list(self.yadisk.listdir(remote_folder))
                        files_list = []
                        for key in files_list_yandex:
                            if key["file"]:
                                files_list.append(remote_folder + key["name"])
                        if remote_path in files_list:
                            logging.warning(
                                f"{__name__}: {str(datetime.now())[:-7]}:    "
                                f'Файл "{local_path.split("/")[-1]}" уже существует в "{remote_folder}" и будет заменен!'
                            )
                            self.yadisk.remove(remote_path, permanently=True)
                        self.yadisk.upload(local_path, remote_path)
                        logging.info(
                            f"{__name__}: {str(datetime.now())[:-7]}:    "
                            f'Файл "{local_path.split("/")[-1]}" отправлен в "{remote_folder}" YaDisk...'
                        )
                else:
                    logging.error(
                        f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка YaDisk: token не валиден"
                    )
                    logger.info(
                        "Ошибка соединения с Yandex.Disc",
                        f"\nОтчеты сохранены в папке {self.local_folder} "
                        f"и не будут отправлены на Yandex.Disc.",
                    )
        else:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    Нет ни одного отчета для отправки в Yandex.Disk"
            )

    def create_path_yadisk(self, path, date_from):
        """
        Проверяет наличие указанного пути в Яндекс Диске. В случае отсутствия каких-либо папок создает их
        :param path:
        :return:
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Проверка путей сохранения файлов на Яндекс.Диске..."
        )
        list_path = path.split("/")
        path = ""
        end_path = ""
        if list_path[-1][-4:] == ".xls" or list_path[-1] == "":
            end_path = list_path.pop()
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
            folders_list_yandex = list(self.yadisk.listdir(directory))
            for key in folders_list_yandex:
                if not key["file"]:
                    folders_list.append(directory + key["name"])
            if folder not in folders_list:
                self.yadisk.mkdir(folder)
                logging.info(
                    f'{__name__}: {str(datetime.now())[:-7]}:    Создание новой папки в YandexDisk - "{folder}"'
                )
                directory = folder + "/"
            else:
                directory = folder + "/"
        path = list_path_yandex[-1] + "/"
        return path

    def export_to_google_sheet(self, date_from):
        """
        Формирование и заполнение google-таблицы
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    "
            f"Сохранение Финансового отчета в Google-таблицах..."
        )

        self.doc_version = 12

        self.sheet_width = 35
        self.sheet2_width = 3
        self.sheet3_width = 14
        self.sheet4_width = 3
        self.sheet5_width = 3
        self.sheet6_width = 16
        self.height = 40
        self.sheet2_height = 40
        self.sheet4_height = 300
        self.sheet5_height = 300
        self.sheet6_height = 40

        # self.CREDENTIALS_FILE # имя файла с закрытым ключом
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.CREDENTIALS_FILE,
            [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        httpAuth = credentials.authorize(httplib2.Http())
        try:
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    "
                f"Попытка авторизации с Google-документами ..."
            )
            self.googleservice = apiclient.discovery.build(
                "sheets", "v4", http=httpAuth
            )
        except IndexError as e:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Ошибка {repr(e)}"
            )

        self.data_report = datetime.strftime(self.finreport_dict["Дата"][0], "%m")
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
            f"{datetime.strftime(self.finreport_dict['Дата'][0], '%Y-%m')} "
            f"({self.data_report}) - Финансовый отчет по Аквапарку"
        )

        if (
            self.finreport_dict["Дата"][0] + timedelta(1)
            != self.finreport_dict["Дата"][1]
        ):
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    "
                f"Экспорт отчета в Google Sheet за несколько дней невозможен!"
            )
            logger.info(
                "Ошибка экспорта в Google.Sheet",
                "Экспорт отчета в Google Sheet за несколько дней невозможен!",
            )
        else:
            with open(self.list_google_docs, "r", encoding="utf-8") as f:
                links = csv.reader(f, delimiter=";")
                self.google_links = {}
                for line in links:
                    self.google_links[line[0]] = [
                        line[1],
                    ]
                    if len(line) > 2:
                        self.google_links[line[0]].append(line[2])
                    else:
                        self.google_links[line[0]].append(0)
            if date_from.strftime("%Y-%m") in self.google_links:
                if (
                    int(self.google_links[date_from.strftime("%Y-%m")][1])
                    == self.doc_version
                ):
                    self.google_doc = (
                        date_from.strftime("%Y-%m"),
                        self.google_links[date_from.strftime("%Y-%m")][0],
                    )
                else:
                    logging.error(
                        f"{__name__}: {str(datetime.now())[:-7]}:    "
                        f"Версия Финансового отчета ("
                        f"{self.google_links[date_from.strftime('%Y-%m')][1]}) "
                        f"не соответствует текущей ({self.doc_version}).\n"
                        f"Необходимо сначала удалить строку с ссылкой на старую версию из файла "
                        f'"list_google_docs.csv" затем заново сформировать отчет с начала месяца.'
                    )
                    logger.info(
                        "Несоответствие версий финансового отчета",
                        f"Версия Финансового отчета ("
                        f"{self.google_links[date_from.strftime('%Y-%m')][1]}) "
                        f"не соответствует текущей ({self.doc_version}).\n"
                        f"Необходимо сначала удалить строку с ссылкой на старую версию из файла "
                        f'"list_google_docs.csv" затем заново сформировать отчет с начала месяца.',
                    )
                    return None
            else:
                self.google_doc = None
                # Создание документа
                self.google_kwote_timer = datetime.now()
                logging.info(
                    f"{__name__}: {str(datetime.now())[:-7]}:    "
                    f"Создание Google-документа..."
                )
                self.spreadsheet = (
                    self.googleservice.spreadsheets()
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
                                            "rowCount": self.height,
                                            "columnCount": self.sheet_width,
                                        },
                                    }
                                },
                                {
                                    "properties": {
                                        "sheetType": "GRID",
                                        "sheetId": 1,
                                        "title": "Смайл",
                                        "gridProperties": {
                                            "rowCount": self.sheet2_height,
                                            "columnCount": self.sheet2_width,
                                        },
                                    }
                                },
                                {
                                    "properties": {
                                        "sheetType": "GRID",
                                        "sheetId": 2,
                                        "title": "План",
                                        "gridProperties": {
                                            "rowCount": self.height,
                                            "columnCount": self.sheet3_width,
                                        },
                                    }
                                },
                                {
                                    "properties": {
                                        "sheetType": "GRID",
                                        "sheetId": 3,
                                        "title": "Итоговый",
                                        "gridProperties": {
                                            "rowCount": self.sheet4_height,
                                            "columnCount": self.sheet4_width,
                                        },
                                    }
                                },
                                {
                                    "properties": {
                                        "sheetType": "GRID",
                                        "sheetId": 4,
                                        "title": "Итоговый ПА",
                                        "gridProperties": {
                                            "rowCount": self.sheet5_height,
                                            "columnCount": self.sheet5_width,
                                        },
                                    }
                                },
                                {
                                    "properties": {
                                        "sheetType": "GRID",
                                        "sheetId": 5,
                                        "title": "Пляж",
                                        "gridProperties": {
                                            "rowCount": self.sheet6_height,
                                            "columnCount": self.sheet6_width,
                                        },
                                    }
                                },
                            ],
                        }
                    )
                    .execute()
                )

                # Доступы к документу
                logging.info(
                    f"{__name__}: {str(datetime.now())[:-7]}:    "
                    f"Настройка доступов к файлу GoogleSheets..."
                )
                self.google_reader_list = self.google_reader_list.split(",")
                self.google_writer_list = self.google_writer_list.split(",")
                driveService = apiclient.discovery.build("drive", "v3", http=httpAuth)
                if self.google_all_read:
                    shareRes = (
                        driveService.permissions()
                        .create(
                            fileId=self.spreadsheet["spreadsheetId"],
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
                for adress in self.google_reader_list:
                    shareRes = (
                        driveService.permissions()
                        .create(
                            fileId=self.spreadsheet["spreadsheetId"],
                            body={
                                "type": "user",
                                "role": "reader",
                                "emailAddress": adress,
                            },
                            fields="id",
                        )
                        .execute()
                    )
                # доступ на Запись определенным пользователоям
                for adress in self.google_writer_list:
                    shareRes = (
                        driveService.permissions()
                        .create(
                            fileId=self.spreadsheet["spreadsheetId"],
                            body={
                                "type": "user",
                                "role": "writer",
                                "emailAddress": adress,
                            },
                            fields="id",
                        )
                        .execute()
                    )

                # ЛИСТ 1
                logging.info(
                    f"{__name__}: {str(datetime.now())[:-7]}:    "
                    f"Создание листа 1 в файле GoogleSheets..."
                )
                sheetId = 0
                # Ширина столбцов
                ss = to_google_sheets.Spreadsheet(
                    self.spreadsheet["spreadsheetId"],
                    sheetId,
                    self.googleservice,
                    self.spreadsheet["sheets"][sheetId]["properties"]["title"],
                )
                ss.prepare_setColumnsWidth(0, 1, 105)
                ss.prepare_setColumnsWidth(2, 9, 120)
                ss.prepare_setColumnWidth(10, 65)
                ss.prepare_setColumnWidth(11, 120)
                ss.prepare_setColumnWidth(12, 100)
                ss.prepare_setColumnsWidth(13, 14, 100)
                ss.prepare_setColumnWidth(15, 65)
                ss.prepare_setColumnWidth(16, 120)
                ss.prepare_setColumnWidth(17, 100)
                ss.prepare_setColumnWidth(18, 65)
                ss.prepare_setColumnWidth(19, 120)
                ss.prepare_setColumnWidth(20, 100)
                ss.prepare_setColumnWidth(21, 65)
                ss.prepare_setColumnWidth(22, 120)
                ss.prepare_setColumnWidth(23, 100)
                ss.prepare_setColumnWidth(24, 65)
                ss.prepare_setColumnWidth(25, 120)
                ss.prepare_setColumnWidth(26, 100)
                ss.prepare_setColumnWidth(27, 65)
                ss.prepare_setColumnWidth(28, 120)
                ss.prepare_setColumnWidth(29, 65)
                ss.prepare_setColumnWidth(30, 120)
                ss.prepare_setColumnWidth(31, 100)
                ss.prepare_setColumnWidth(32, 120)
                ss.prepare_setColumnWidth(33, 120)
                ss.prepare_setColumnWidth(34, 120)

                # Объединение ячеек
                ss.prepare_mergeCells("A1:A2")
                ss.prepare_mergeCells("B1:B2")
                ss.prepare_mergeCells("C1:C2")
                ss.prepare_mergeCells("D1:D2")
                ss.prepare_mergeCells("E1:E2")
                ss.prepare_mergeCells("F1:F2")
                ss.prepare_mergeCells("G1:G2")
                ss.prepare_mergeCells("H1:H2")
                ss.prepare_mergeCells("I1:I2")
                ss.prepare_mergeCells("J1:J2")
                ss.prepare_mergeCells("K1:M1")
                ss.prepare_mergeCells("N1:N2")
                ss.prepare_mergeCells("O1:O2")
                ss.prepare_mergeCells("P1:R1")
                ss.prepare_mergeCells("S1:U1")
                ss.prepare_mergeCells("V1:X1")
                ss.prepare_mergeCells("Y1:AA1")
                ss.prepare_mergeCells("AB1:AC1")
                ss.prepare_mergeCells("AD1:AF1")
                ss.prepare_mergeCells("AG1:AG2")
                ss.prepare_mergeCells("AH1:AH2")
                ss.prepare_mergeCells("AI1:AI2")

                # Задание параметров группе ячеек
                # Жирный, по центру
                ss.prepare_setCellsFormat(
                    "A1:AI2",
                    {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
                )
                # ss.prepare_setCellsFormat('E4:E8', {'numberFormat': {'pattern': '[h]:mm:ss', 'type': 'TIME'}},
                #                           fields='userEnteredFormat.numberFormat')

                # Заполнение таблицы
                ss.prepare_setValues(
                    "A1:AI2",
                    [
                        [
                            "Дата",
                            "День недели",
                            "Кол-во проходов \nПЛАН",
                            "Кол-во проходов \nФАКТ",
                            f"Кол-во проходов \n{self.data_report} "
                            f"{datetime.strftime(self.finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
                            "Общая сумма \nПЛАН",
                            "Общая сумма \nФАКТ",
                            "Средний чек \nФАКТ",
                            "Бонусы",
                            f"Общая сумма \n{self.data_report} "
                            f"{datetime.strftime(self.finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
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
                            f"Общепит {self.data_report} "
                            f"{datetime.strftime(self.finreport_dict['Дата'][0] - relativedelta(years=1), '%Y')}",
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
                            "Сумма безнал",
                            "Фотоуслуги",
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
                            "Кол-во",
                            "Сумма",
                            "Средний чек",
                            "",
                            "",
                            "",
                        ],
                    ],
                    "ROWS",
                )
                # ss.prepare_setValues("D5:E6", [["This is D5", "This is D6"], ["This is E5", "=5+5"]], "COLUMNS")

                # Цвет фона ячеек
                ss.prepare_setCellsFormat(
                    "A1:AI2",
                    {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
                    fields="userEnteredFormat.backgroundColor",
                )

                # Бордер
                for i in range(2):
                    for j in range(self.sheet_width):
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
                ss = to_google_sheets.Spreadsheet(
                    self.spreadsheet["spreadsheetId"],
                    sheetId,
                    self.googleservice,
                    self.spreadsheet["sheets"][sheetId]["properties"]["title"],
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
                    for j in range(self.sheet2_width):
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
                ss = to_google_sheets.Spreadsheet(
                    self.spreadsheet["spreadsheetId"],
                    sheetId,
                    self.googleservice,
                    self.spreadsheet["sheets"][sheetId]["properties"]["title"],
                )
                ss.prepare_setColumnsWidth(0, 1, 100)
                ss.prepare_setColumnsWidth(2, 7, 120)
                ss.prepare_setColumnWidth(8, 65)
                ss.prepare_setColumnWidth(9, 120)
                ss.prepare_setColumnWidth(10, 100)
                ss.prepare_setColumnWidth(8, 65)
                ss.prepare_setColumnWidth(9, 120)
                ss.prepare_setColumnWidth(10, 100)

                # Объединение ячеек
                ss.prepare_mergeCells("A1:A2")
                ss.prepare_mergeCells("B1:B2")
                ss.prepare_mergeCells("C1:C2")
                ss.prepare_mergeCells("D1:D2")
                ss.prepare_mergeCells("E1:E2")
                ss.prepare_mergeCells("F1:F2")
                ss.prepare_mergeCells("G1:G2")
                ss.prepare_mergeCells("H1:H2")
                ss.prepare_mergeCells("I1:K1")
                ss.prepare_mergeCells("L1:N1")

                # Задание параметров группе ячеек
                # Жирный, по центру
                ss.prepare_setCellsFormat(
                    "A1:N2",
                    {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
                )
                # ss.prepare_setCellsFormat('E4:E8', {'numberFormat': {'pattern': '[h]:mm:ss', 'type': 'TIME'}},
                #                           fields='userEnteredFormat.numberFormat')

                # Заполнение таблицы
                ss.prepare_setValues(
                    "A1:N2",
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
                    "A1:N2",
                    {"backgroundColor": functions.htmlColorToJSON("#f7cb4d")},
                    fields="userEnteredFormat.backgroundColor",
                )

                # Бордер
                for i in range(2):
                    for j in range(self.sheet3_width):
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
                # ss = to_google_sheets.Spreadsheet(self.spreadsheet['spreadsheetId'], sheetId, self.googleservice,
                #                                   self.spreadsheet['sheets'][sheetId]['properties']['title'])

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
                    f"01{self.finreport_dict['Дата'][0].strftime('%m%Y')}", "%d%m%Y"
                )
                enddate = start_date + relativedelta(months=1)
                dateline = start_date
                self.sheet2_line = 3
                while dateline < enddate:
                    ss.prepare_setValues(
                        f"A{self.sheet2_line}:N{self.sheet2_line}",
                        [
                            [
                                datetime.strftime(dateline, "%d.%m.%Y"),
                                weekday_rus[dateline.weekday()],
                                "",
                                f"=IF(OR('Сводный'!A{self.sheet2_line} = \"ИТОГО\";LEFT('Сводный'!A{self.sheet2_line}; 10) = \"Выполнение\");\"\";'Сводный'!D{self.sheet2_line})",
                                "",
                                f"=IF(OR('Сводный'!A{self.sheet2_line} = \"ИТОГО\";LEFT('Сводный'!A{self.sheet2_line}; 10) = \"Выполнение\");\"\";'Сводный'!G{self.sheet2_line})",
                                f"=IFERROR(E{self.sheet2_line}/C{self.sheet2_line};0)",
                                f"=IFERROR(F{self.sheet2_line}/D{self.sheet2_line};0)",
                                "",
                                "",
                                f"=IFERROR(J{self.sheet2_line}/I{self.sheet2_line};0)",
                                "",
                                "",
                                f"=IFERROR(M{self.sheet2_line}/L{self.sheet2_line};0)",
                            ]
                        ],
                        "ROWS",
                    )

                    # Задание форматы вывода строки
                    ss.prepare_setCellsFormats(
                        f"A{self.sheet2_line}:N{self.sheet2_line}",
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
                            ]
                        ],
                    )
                    # Цвет фона ячеек
                    if self.sheet2_line % 2 != 0:
                        ss.prepare_setCellsFormat(
                            f"A{self.sheet2_line}:N{self.sheet2_line}",
                            {"backgroundColor": functions.htmlColorToJSON("#fef8e3")},
                            fields="userEnteredFormat.backgroundColor",
                        )

                    # Бордер
                    for j in range(self.sheet3_width):
                        ss.requests.append(
                            {
                                "updateBorders": {
                                    "range": {
                                        "sheetId": ss.sheetId,
                                        "startRowIndex": self.sheet2_line - 1,
                                        "endRowIndex": self.sheet2_line,
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
                                        "startRowIndex": self.sheet2_line - 1,
                                        "endRowIndex": self.sheet2_line,
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
                                        "startRowIndex": self.sheet2_line - 1,
                                        "endRowIndex": self.sheet2_line,
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
                                        "startRowIndex": self.sheet2_line - 1,
                                        "endRowIndex": self.sheet2_line,
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
                    self.sheet2_line += 1
                    dateline += timedelta(1)

                # ИТОГО
                ss.prepare_setValues(
                    f"A{self.sheet2_line}:N{self.sheet2_line}",
                    [
                        [
                            "ИТОГО",
                            "",
                            f"=SUM(C3:C{self.sheet2_line - 1})",
                            f"=SUM(D3:D{self.sheet2_line - 1})",
                            f"=SUM(E3:E{self.sheet2_line - 1})",
                            f"=SUM(F3:F{self.sheet2_line - 1})",
                            f"=IFERROR(E{self.sheet2_line}/C{self.sheet2_line};0)",
                            f"=IFERROR(F{self.sheet2_line}/D{self.sheet2_line};0)",
                            f"=SUM(I3:I{self.sheet2_line - 1})",
                            f"=SUM(J3:J{self.sheet2_line - 1})",
                            f"=IFERROR(J{self.sheet2_line}/I{self.sheet2_line};0)",
                            f"=SUM(L3:L{self.sheet2_line - 1})",
                            f"=SUM(M3:M{self.sheet2_line - 1})",
                            f"=IFERROR(M{self.sheet2_line}/L{self.sheet2_line};0)",
                        ]
                    ],
                    "ROWS",
                )

                # Задание форматы вывода строки
                ss.prepare_setCellsFormats(
                    f"A{self.sheet2_line}:N{self.sheet2_line}",
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
                        ]
                    ],
                )
                # Цвет фона ячеек
                ss.prepare_setCellsFormat(
                    f"A{self.sheet2_line}:N{self.sheet2_line}",
                    {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
                    fields="userEnteredFormat.backgroundColor",
                )

                # Бордер
                for j in range(self.sheet3_width):
                    ss.requests.append(
                        {
                            "updateBorders": {
                                "range": {
                                    "sheetId": ss.sheetId,
                                    "startRowIndex": self.sheet2_line - 1,
                                    "endRowIndex": self.sheet2_line,
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
                                    "startRowIndex": self.sheet2_line - 1,
                                    "endRowIndex": self.sheet2_line,
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
                                    "startRowIndex": self.sheet2_line - 1,
                                    "endRowIndex": self.sheet2_line,
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
                                    "startRowIndex": self.sheet2_line - 1,
                                    "endRowIndex": self.sheet2_line,
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
                ss = to_google_sheets.Spreadsheet(
                    self.spreadsheet["spreadsheetId"],
                    sheetId,
                    self.googleservice,
                    self.spreadsheet["sheets"][sheetId]["properties"]["title"],
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
                    f"A1:C1",
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
                for j in range(self.sheet4_width):
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
                ss = to_google_sheets.Spreadsheet(
                    self.spreadsheet["spreadsheetId"],
                    sheetId,
                    self.googleservice,
                    self.spreadsheet["sheets"][sheetId]["properties"]["title"],
                )
                ss.prepare_setColumnWidth(0, 300)
                ss.prepare_setColumnsWidth(1, 2, 160)

                ss.prepare_setValues(
                    "A1:C1",
                    [
                        [
                            '=JOIN(" ";"Итоговый отчет платежного агента будет сформирован через";DATEDIF(TODAY();DATE(YEAR(TODAY());'
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
                    f"A1:C1",
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
                for j in range(self.sheet4_width):
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
                ss = to_google_sheets.Spreadsheet(
                    self.spreadsheet["spreadsheetId"],
                    sheetId,
                    self.googleservice,
                    self.spreadsheet["sheets"][sheetId]["properties"]["title"],
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
                    for j in range(self.sheet6_width):
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

                self.google_doc = (
                    date_from.strftime("%Y-%m"),
                    self.spreadsheet["spreadsheetId"],
                )
                self.google_links[self.google_doc[0]] = [
                    self.google_doc[1],
                    self.doc_version,
                ]
                links = []
                for docid in self.google_links:
                    links.append(
                        [
                            docid,
                            self.google_links[docid][0],
                            self.google_links[docid][1],
                        ]
                    )
                with open(
                    self.list_google_docs, "w", newline="", encoding="utf-8"
                ) as f:
                    file = csv.writer(f, delimiter=";")
                    for link in links:
                        file.writerow(link)
                logging.info(
                    f'{__name__}: {str(datetime.now())[:-7]}:    Создана новая таблица с Id: {self.spreadsheet["spreadsheetId"]}'
                )

            self.spreadsheet = (
                self.googleservice.spreadsheets()
                .get(spreadsheetId=self.google_doc[1], ranges=[], includeGridData=True)
                .execute()
            )

            # -------------------------------- ЗАПОЛНЕНИЕ ДАННЫМИ ------------------------------------------------

            # Печать таблицы в консоль
            # s = ''
            # for line_table in spreadsheet['sheets'][0]['data'][0]['rowData']:
            #     for cell in line_table['values']:
            #         try:
            #             s += cell['formattedValue'] + " | "
            #         except KeyError:
            #             pass
            #     s = ''

            # Проверка нет ли текущей даты в таблице
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    "
                f"Проверка нет ли текущей даты в таблице..."
            )
            self.start_line = 1
            self.reprint = 2

            for line_table in self.spreadsheet["sheets"][0]["data"][0]["rowData"]:
                try:
                    if line_table["values"][0]["formattedValue"] == datetime.strftime(
                        self.finreport_dict["Дата"][0], "%d.%m.%Y"
                    ):
                        self.rewrite_google_sheet()
                        self.reprint = 0
                        break
                    elif line_table["values"][0]["formattedValue"] == "ИТОГО":
                        break
                    else:
                        self.start_line += 1
                except KeyError:
                    self.start_line += 1
            if self.reprint:
                self.write_google_sheet()
            # width_table = len(self.spreadsheet['sheets'][0]['data'][0]['rowData'][0]['values'])
        return True

    def rewrite_google_sheet(self):
        """
        Заполнение google-таблицы в случае, если данные уже существуют
        """
        logging.warning(
            f"{__name__}: {str(datetime.now())[:-7]}:    Перезапись уже существующей строки..."
        )
        self.reprint = 1
        self.write_google_sheet()

    def write_google_sheet(self):
        """
        Заполнение google-таблицы
        """
        # SHEET 1
        # try:
        #     while True:
        #         time_of = (datetime.now() - self.google_kwote_timer).seconds
        #         if time_of < 100:
        #             logging.info(f'{__name__}: {str(datetime.now())[:-7]}:    '
        #                          f'Превышено количество запросов в API GoogleSheets. \n'
        #                          f'Программа продолжит выполнение через {100-time_of} сек...')
        #             time.sleep(5)
        #         else:
        #             break
        # except AttributeError:
        #     pass

        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    " f"Заполнение листа 1..."
        )
        sheetId = 0
        ss = to_google_sheets.Spreadsheet(
            self.spreadsheet["spreadsheetId"],
            sheetId,
            self.googleservice,
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
                self.finreport_dict["Билеты аквапарка"][1],
                self.finreport_dict["Общепит"][1],
                self.finreport_dict["Билеты аквапарка КОРП"][1],
                self.finreport_dict["Прочее"][1],
                self.finreport_dict["Сопутствующие товары"][1],
                self.finreport_dict["Депозит"][1],
                self.finreport_dict["Штраф"][1],
                self.finreport_dict["Online Продажи"][1],
                self.finreport_dict["Фотоуслуги"][1],
            ]
        )

        if self.finreport_dict["ИТОГО"][1] != control_total_sum:
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    Несоответствие данных: Сумма услуг не равна итоговой сумме"
            )
            logger.info(
                "Несоответствие данных",
                f"Сумма услуг по группам + депозит ({control_total_sum}) не равна итоговой сумме "
                f"({self.finreport_dict['ИТОГО'][1]}). \n"
                f"Рекомендуется проверить правильно ли разделены услуги по группам.",
            )

        ss.prepare_setValues(
            f"A{self.nex_line}:AI{self.nex_line}",
            [
                [
                    datetime.strftime(self.finreport_dict["Дата"][0], "%d.%m.%Y"),
                    weekday_rus[self.finreport_dict["Дата"][0].weekday()],
                    f"='План'!C{self.nex_line}",
                    f"{self.finreport_dict['Кол-во проходов'][0]}",
                    f"{self.finreport_dict_lastyear['Кол-во проходов'][0]}",
                    f"='План'!E{self.nex_line}",
                    f"={str(self.finreport_dict['ИТОГО'][1]).replace('.', ',')}"
                    f"-I{self.nex_line}+AG{self.nex_line}+AI{self.nex_line}+'Смайл'!C{self.nex_line}",
                    f"=IFERROR(G{self.nex_line}/D{self.nex_line};0)",
                    f"={str(self.finreport_dict['MaxBonus'][1]).replace('.', ',')}",
                    f"={str(self.finreport_dict_lastyear['ИТОГО'][1]).replace('.', ',')}"
                    f"-{str(self.finreport_dict_lastyear['MaxBonus'][1]).replace('.', ',')}"
                    f"+{str(self.finreport_dict_lastyear['Online Продажи'][1]).replace('.', ',')}",
                    self.finreport_dict["Билеты аквапарка"][0],
                    self.finreport_dict["Билеты аквапарка"][1],
                    f"=IFERROR(L{self.nex_line}/K{self.nex_line};0)",
                    self.finreport_dict["Депозит"][1],
                    self.finreport_dict["Штраф"][1],
                    f"='План'!I{self.nex_line}",
                    f"='План'!J{self.nex_line}",
                    f"=IFERROR(Q{self.nex_line}/P{self.nex_line};0)",
                    self.finreport_dict["Общепит"][0] + self.finreport_dict["Смайл"][0],
                    self.finreport_dict["Общепит"][1] + self.finreport_dict["Смайл"][1],
                    f"=IFERROR(T{self.nex_line}/S{self.nex_line};0)",
                    self.finreport_dict_lastyear["Общепит"][0]
                    + self.finreport_dict_lastyear["Смайл"][0],
                    self.finreport_dict_lastyear["Общепит"][1]
                    + self.finreport_dict_lastyear["Смайл"][1],
                    f"=IFERROR(W{self.nex_line}/V{self.nex_line};0)",
                    self.finreport_dict["Билеты аквапарка КОРП"][0],
                    self.finreport_dict["Билеты аквапарка КОРП"][1],
                    f"=IFERROR(Z{self.nex_line}/Y{self.nex_line};0)",
                    self.finreport_dict["Прочее"][0]
                    + self.finreport_dict["Сопутствующие товары"][0],
                    self.finreport_dict["Прочее"][1]
                    + self.finreport_dict["Сопутствующие товары"][1],
                    self.finreport_dict["Online Продажи"][0],
                    self.finreport_dict["Online Продажи"][1],
                    f"=IFERROR(AE{self.nex_line}/AD{self.nex_line};0)",
                    0,
                    self.finreport_dict["Фотоуслуги"][1],
                    0,
                ]
            ],
            "ROWS",
        )

        # Задание форматы вывода строки
        ss.prepare_setCellsFormats(
            f"A{self.nex_line}:AI{self.nex_line}",
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
                    {"numberFormat": {}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                    {"numberFormat": {"type": "CURRENCY", "pattern": "#0[$ ₽]"}},
                ]
            ],
        )
        # Цвет фона ячеек
        if self.nex_line % 2 != 0:
            ss.prepare_setCellsFormat(
                f"A{self.nex_line}:AI{self.nex_line}",
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
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    "
            f"Заполнение строки ИТОГО на листе 1..."
        )

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
            f"A{height_table}:AI{height_table}",
            [
                [
                    f"ИТОГО",
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
                    f"=SUM(AD3:AD{height_table - 1})",
                    f"=SUM(AE3:AE{height_table - 1})",
                    f"=IFERROR(ROUND(AE{height_table}/AD{height_table};2);0)",
                    f"=SUM(AG3:AG{height_table - 1})",
                    f"=SUM(AH3:AH{height_table - 1})",
                    f"=SUM(AI3:AI{height_table - 1})",
                ]
            ],
            "ROWS",
        )
        ss.prepare_setValues(
            f"A{height_table + 1}:D{height_table + 1}",
            [
                [
                    f"Выполнение плана (трафик)",
                    "",
                    f"=IFERROR('План'!C{self.sheet2_line};0)",
                    f"=IFERROR(ROUND(D{height_table}/C{height_table+1};2);0)",
                ]
            ],
            "ROWS",
        )
        ss.prepare_setValues(
            f"A{height_table + 2}:D{height_table + 2}",
            [
                [
                    f"Выполнение плана (доход)",
                    "",
                    f"=IFERROR('План'!E{self.sheet2_line};0)",
                    f"=IFERROR(ROUND(G{height_table}/C{height_table+2};2);0)",
                ]
            ],
            "ROWS",
        )

        # Задание формата вывода строки
        ss.prepare_setCellsFormats(
            f"A{height_table}:AI{height_table}",
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
            f"A{height_table+2}:D{height_table+2}",
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
            f"A{height_table}:AI{height_table}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )
        ss.prepare_setCellsFormat(
            f"A{height_table+1}:D{height_table+1}",
            {"backgroundColor": functions.htmlColorToJSON("#fce8b2")},
            fields="userEnteredFormat.backgroundColor",
        )
        ss.prepare_setCellsFormat(
            f"A{height_table+2}:D{height_table+2}",
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

        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    " f"Заполнение листа 2..."
        )
        sheetId = 1
        ss = to_google_sheets.Spreadsheet(
            self.spreadsheet["spreadsheetId"],
            sheetId,
            self.googleservice,
            self.spreadsheet["sheets"][sheetId]["properties"]["title"],
        )

        self.nex_line = self.start_line

        ss.prepare_setValues(
            f"A{self.nex_line}:C{self.nex_line}",
            [
                [
                    datetime.strftime(self.finreport_dict["Дата"][0], "%d.%m.%Y"),
                    len(self.report_rk),
                    float(sum([line["paid_sum"] for line in self.report_rk])),
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
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    "
            f"Заполнение строки ИТОГО на листе 2..."
        )

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
                    f"ИТОГО",
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
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    " f"Заполнение  листа 4..."
            )
            sheetId = 3
            ss = to_google_sheets.Spreadsheet(
                self.spreadsheet["spreadsheetId"],
                sheetId,
                self.googleservice,
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
                        f"За {self.data_report} {datetime.strftime(self.finreport_dict['Дата'][0], '%Y')}",
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

            for group in self.finreport_dict_month:
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
                            self.finreport_dict_month[group]["Итого по группе"][0][1],
                            self.finreport_dict_month[group]["Итого по группе"][0][2],
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
                for folder in self.finreport_dict_month[group]:
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
                                self.finreport_dict_month[group][folder][0][1],
                                self.finreport_dict_month[group][folder][0][2],
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
                    for servise in self.finreport_dict_month[group][folder]:
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
            logging.info(
                f"{__name__}: {str(datetime.now())[:-7]}:    " f"Заполнение  листа 5..."
            )
            sheetId = 4
            ss = to_google_sheets.Spreadsheet(
                self.spreadsheet["spreadsheetId"],
                sheetId,
                self.googleservice,
                self.spreadsheet["sheets"][sheetId]["properties"]["title"],
            )

            self.nex_line = 1
            ss.prepare_setValues(
                f"A{self.nex_line}:C{self.nex_line}",
                [[f"Итоговый отчет платежного агента", "", ""]],
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
                        f"За {self.data_report} {datetime.strftime(self.finreport_dict['Дата'][0], '%Y')}",
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
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    " f"Заполнение листа 6..."
        )
        sheetId = 5
        ss = to_google_sheets.Spreadsheet(
            self.spreadsheet["spreadsheetId"],
            sheetId,
            self.googleservice,
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
                    datetime.strftime(self.finreport_dict_beach["Дата"][0], "%d.%m.%Y"),
                    weekday_rus[self.finreport_dict_beach["Дата"][0].weekday()],
                    f"='План'!L{self.nex_line}",
                    self.finreport_dict_beach["Выход с пляжа"][0],
                    f"='План'!M{self.nex_line}",
                    str(self.finreport_dict_beach["Итого по отчету"][1]).replace(
                        ".", ","
                    ),
                    self.finreport_dict_beach["Депозит"][1],
                    self.finreport_dict_beach["Карты"][0],
                    self.finreport_dict_beach["Карты"][1],
                    f"=IFERROR(I{self.nex_line}/H{self.nex_line};0)",
                    self.finreport_dict_beach["Услуги"][0],
                    self.finreport_dict_beach["Услуги"][1],
                    f"=IFERROR(L{self.nex_line}/K{self.nex_line};0)",
                    self.finreport_dict_beach["Товары"][0],
                    self.finreport_dict_beach["Товары"][1],
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
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    "
            f"Заполнение строки ИТОГО на листе 2..."
        )

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
                    f"ИТОГО",
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
                    f"Выполнение плана (трафик)",
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
                    f"Выполнение плана (доход)",
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

    def sms_report(self, date_from):
        """
        Составляет текстовую версию финансового отчета
        :return: str
        """
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Составление SMS-отчета..."
        )
        resporse = "Отчет по аквапарку за "

        if self.finreport_dict["Дата"][0] == self.finreport_dict["Дата"][1] - timedelta(
            1
        ):
            resporse += (
                f'{datetime.strftime(self.finreport_dict["Дата"][0], "%d.%m.%Y")}:\n'
            )

        else:
            resporse += (
                f'{datetime.strftime(self.finreport_dict["Дата"][0], "%d.%m.%Y")} '
                f'- {datetime.strftime(self.finreport_dict["Дата"][1] - timedelta(1), "%d.%m.%Y")}:\n'
            )

        def get_sum(field_name: str) -> float:
            return self.finreport_dict.get(field_name, [0, 0])[1]

        bars_sum = get_sum("ИТОГО")
        smile = get_sum("Смайл")
        bonuses = get_sum("MaxBonus")
        other = get_sum("Прочее") + get_sum("Сопутствующие товары")
        total_sum = bars_sum - bonuses + smile

        if self.finreport_dict["ИТОГО"][1]:
            resporse += f'Люди - {self.finreport_dict["Кол-во проходов"][0]};\n'
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

        if self.itog_report_org2["Итого по отчету"][1]:
            # resporse += 'Отчет по пляжу за '
            # if beach_report['Дата'][0] + timedelta(1) == beach_report['Дата'][1]:
            #     resporse += f'{datetime.strftime(beach_report["Дата"][0], "%d.%m.%Y")}:\n'
            # else:
            #     resporse += f'{datetime.strftime(beach_report["Дата"][0], "%d.%m.%Y")} - {datetime.strftime(beach_report["Дата"][0], "%d.%m.%Y")}:\n'
            try:
                resporse += f'Люди (пляж) - {self.itog_report_org2["Летняя зона | БЕЗЛИМИТ | 1 проход"][0]};\n'
            except KeyError:
                pass
            resporse += f'Итого по пляжу - {self.itog_report_org2["Итого по отчету"][1]:.2f} ₽;\n'

        resporse += "Без ЧП."

        with open(
            f'reports/{date_from.strftime("%Y.%m.%d")}_sms.txt', "w", encoding="utf-8"
        ) as f:
            f.write(resporse)
        return resporse

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

        fill = PatternFill(fill_type="solid", start_color="c1c1c1", end_color="c2c2c2")
        align_top = Alignment(
            horizontal="general",
            vertical="top",
            text_rotation=0,
            wrap_text=False,
            shrink_to_fit=False,
            indent=0,
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
        align_center = Alignment(
            horizontal="center",
            vertical="bottom",
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
        ws[column[3] + self.row].font = font
        ws[column[3] + self.row].alignment = align_top
        ws[column[1] + next_row()] = ""

        ws[column[3] + next_row()] = "За период с:"
        ws[column[3] + self.row].font = font
        ws[column[3] + self.row].alignment = align_top
        ws[column[5] + self.row] = itog_report["Дата"][0].strftime("%d.%m.%Y")
        ws[column[5] + self.row].font = font_bold
        ws[column[5] + self.row].alignment = align_top
        ws[column[7] + self.row] = "По:"
        ws[column[7] + self.row].font = font
        ws[column[7] + self.row].alignment = align_top
        ws[column[9] + self.row] = (itog_report["Дата"][1] - timedelta(1)).strftime(
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

        ws[column[2] + next_row()] = "Название"
        ws[column[10] + self.row] = "Количество"
        ws[column[12] + self.row] = "Сумма"
        merge_table()
        ws[column[2] + self.row].font = h3
        ws[column[10] + self.row].font = h3
        ws[column[12] + self.row].font = h3
        ws[column[2] + self.row].alignment = align_top
        ws[column[10] + self.row].alignment = align_top
        ws[column[12] + self.row].alignment = align_top

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
            logging.error(
                f"{__name__}: {str(datetime.now())[:-7]}:    {error_code} {error_message}"
            )
            logger.info(error_code, error_message)
            return None

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

        # сохранение файла в текущую директорию
        if itog_report["Дата"][0] == itog_report["Дата"][1] - timedelta(1):
            date_ = datetime.strftime(itog_report["Дата"][0], "%Y-%m-%d")
        else:
            date_ = (
                f'{datetime.strftime(itog_report["Дата"][0], "%Y-%m-%d")} - '
                f'{datetime.strftime(itog_report["Дата"][1] - timedelta(1), "%Y-%m-%d")}'
            )
        path = (
            self.local_folder
            + self.report_path
            + date_
            + f' Итоговый отчет по {organisation_total["Организация"]["Организация"][0][0]} '
            + ".xlsx"
        )
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Сохранение Итогового отчета "
            f'по {organisation_total["Организация"]["Организация"][0][0]} в {path}'
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def save_cashdesk_report(self, cashdesk_report, date_from):
        """
        Сохраняет Суммовой отчет в Excel
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
        h3 = Font(
            name="Times New Roman",
            size=10,
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
        border_right = Border(right=Side(border_style="thin", color="FF000000"))
        border_left = Border(left=Side(border_style="thin", color="FF000000"))
        align_left = Alignment(
            horizontal="left",
            vertical="bottom",
            text_rotation=0,
            wrap_text=False,
            shrink_to_fit=False,
            indent=0,
        )
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
        ws["A1"].font = h1
        # выравнивание
        ws["A1"].alignment = align_left

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
        ws[column[1] + self.row].font = h1
        # выравнивание
        ws[column[1] + self.row].alignment = align_left
        # Высота строк
        ws.row_dimensions[1].height = 24

        ws[column[1] + next_row()] = f'{cashdesk_report["Организация"][0][0]}'
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        ws[column[1] + self.row].font = font
        ws[column[1] + self.row].alignment = align_top

        ws[column[1] + next_row()] = "За период с:"
        ws[column[1] + self.row].font = font
        ws[column[1] + self.row].alignment = align_top
        ws[column[2] + self.row] = cashdesk_report["Дата"][0][0].strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=2, end_row=self.row, end_column=3
        )
        ws[column[2] + self.row].font = font_bold
        ws[column[2] + self.row].alignment = align_top
        ws[column[4] + self.row] = "по"
        ws[column[4] + self.row].font = font
        ws[column[4] + self.row].alignment = align_top
        ws[column[5] + self.row] = (
            cashdesk_report["Дата"][0][1] - timedelta(1)
        ).strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=5, end_row=self.row, end_column=7
        )
        ws[column[5] + self.row].font = font_bold
        ws[column[5] + self.row].alignment = align_top

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
            ws[column[1] + self.row].font = font
            ws[column[3] + self.row].font = font
            ws[column[7] + self.row].font = font
            ws[column[10] + self.row].font = font
            ws[column[11] + self.row].font = font
            ws[column[13] + self.row].font = font
            ws[column[14] + self.row].font = font
            ws[column[1] + self.row].alignment = align_top
            ws[column[3] + self.row].alignment = align_top
            ws[column[7] + self.row].alignment = align_top
            ws[column[10] + self.row].alignment = align_top
            ws[column[11] + self.row].alignment = align_top
            ws[column[13] + self.row].alignment = align_top
            ws[column[14] + self.row].alignment = align_top
            ws[column[3] + self.row].number_format = "#,##0.00 ₽"
            ws[column[7] + self.row].number_format = "#,##0.00 ₽"
            ws[column[10] + self.row].number_format = "#,##0.00 ₽"
            ws[column[11] + self.row].number_format = "#,##0.00 ₽"
            ws[column[13] + self.row].number_format = "#,##0.00 ₽"
            ws[column[14] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
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
            ws[column[1] + self.row].font = h3
            ws[column[3] + self.row].font = h3
            ws[column[7] + self.row].font = h3
            ws[column[10] + self.row].font = h3
            ws[column[11] + self.row].font = h3
            ws[column[13] + self.row].font = h3
            ws[column[14] + self.row].font = h3
            ws[column[1] + self.row].alignment = align_top
            ws[column[3] + self.row].alignment = align_top
            ws[column[7] + self.row].alignment = align_top
            ws[column[10] + self.row].alignment = align_top
            ws[column[11] + self.row].alignment = align_top
            ws[column[13] + self.row].alignment = align_top
            ws[column[14] + self.row].alignment = align_top
            ws[column[3] + self.row].number_format = "#,##0.00 ₽"
            ws[column[7] + self.row].number_format = "#,##0.00 ₽"
            ws[column[10] + self.row].number_format = "#,##0.00 ₽"
            ws[column[11] + self.row].number_format = "#,##0.00 ₽"
            ws[column[13] + self.row].number_format = "#,##0.00 ₽"
            ws[column[14] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
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
            ws[column[1] + self.row].font = font_red
            ws[column[3] + self.row].font = font_red
            ws[column[7] + self.row].font = font_red
            ws[column[10] + self.row].font = font_red
            ws[column[11] + self.row].font = font_red
            ws[column[13] + self.row].font = font_red
            ws[column[14] + self.row].font = font_red
            ws[column[1] + self.row].alignment = align_top
            ws[column[3] + self.row].alignment = align_top
            ws[column[7] + self.row].alignment = align_top
            ws[column[10] + self.row].alignment = align_top
            ws[column[11] + self.row].alignment = align_top
            ws[column[13] + self.row].alignment = align_top
            ws[column[14] + self.row].alignment = align_top
            ws[column[3] + self.row].number_format = "#,##0.00 ₽"
            ws[column[7] + self.row].number_format = "#,##0.00 ₽"
            ws[column[10] + self.row].number_format = "#,##0.00 ₽"
            ws[column[11] + self.row].number_format = "#,##0.00 ₽"
            ws[column[13] + self.row].number_format = "#,##0.00 ₽"
            ws[column[14] + self.row].number_format = "#,##0.00 ₽"
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
                b += 1

        def merge_width_red():
            ws.merge_cells(
                start_row=self.row,
                start_column=1,
                end_row=self.row,
                end_column=len(column) - 1,
            )
            ws[column[1] + self.row].font = font_red
            ws[column[1] + self.row].alignment = align_top
            b = 1
            while b < len(column):
                if b == 1:
                    ws[column[b] + self.row].border = border_left
                elif b == len(column) - 1:
                    ws[column[b] + self.row].border = border_right
                else:
                    ws[column[b] + self.row].border = border
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
            ws[column[b] + self.row].fill = fill
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
            self.local_folder
            + self.report_path
            + date_
            + f' Суммовой отчет по {cashdesk_report["Организация"][0][0]}'
            + ".xlsx"
        )
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Сохранение Суммового отчета "
            f'по {cashdesk_report["Организация"][0][0]} в {path}'
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def save_client_count_totals(self, client_count_totals_org, date_from):
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
        ws["A1"].font = h1
        # выравнивание
        ws["A1"].alignment = align_left

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
        ws[column[1] + self.row].font = h1
        # выравнивание
        ws[column[1] + self.row].alignment = align_left
        # Высота строк
        ws.row_dimensions[1].height = 24

        ws[column[1] + next_row()] = f"{client_count_totals_org[0][0]}"
        ws.merge_cells(
            start_row=self.row,
            start_column=1,
            end_row=self.row,
            end_column=len(column) - 1,
        )
        ws[column[1] + self.row].font = font
        ws[column[1] + self.row].alignment = align_top

        ws[column[1] + next_row()] = "За период с:"
        ws[column[1] + self.row].font = font
        ws[column[1] + self.row].alignment = align_top
        ws[column[2] + self.row] = client_count_totals_org[1][0].strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=2, end_row=self.row, end_column=3
        )
        ws[column[2] + self.row].font = font_bold
        ws[column[2] + self.row].alignment = align_top
        ws[column[4] + self.row] = "по"
        ws[column[4] + self.row].font = font
        ws[column[4] + self.row].alignment = align_top
        ws[column[5] + self.row] = (client_count_totals_org[-2][0]).strftime("%d.%m.%Y")
        ws.merge_cells(
            start_row=self.row, start_column=5, end_row=self.row, end_column=7
        )
        ws[column[5] + self.row].font = font_bold
        ws[column[5] + self.row].alignment = align_top

        # ТАБЛИЦА
        def merge_table():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=5
            )
            ws[column[1] + self.row].font = font
            ws[column[3] + self.row].font = font
            ws[column[1] + self.row].alignment = align_top
            ws[column[3] + self.row].alignment = align_top
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
                b += 1

        def merge_table_bold():
            ws.merge_cells(
                start_row=self.row, start_column=1, end_row=self.row, end_column=2
            )
            ws.merge_cells(
                start_row=self.row, start_column=3, end_row=self.row, end_column=5
            )
            ws[column[1] + self.row].font = font_bold
            ws[column[3] + self.row].font = font_bold
            ws[column[1] + self.row].alignment = align_top
            ws[column[3] + self.row].alignment = align_top
            b = 1
            while b < len(column):
                ws[column[b] + self.row].border = border
                b += 1

        ws[column[1] + next_row()] = "Дата"
        ws[column[3] + self.row] = "Количество клиентов"
        merge_table_bold()
        # раскрвшивание фона для заголовков
        b = 1
        while b < len(column):
            ws[column[b] + self.row].fill = fill
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
            self.local_folder
            + self.report_path
            + date_
            + f" Количество клиентов за день по {client_count_totals_org[0][0]}"
            + ".xlsx"
        )
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Сохранение отчета по количеству клиентов "
            f"по {client_count_totals_org[0][0]} в {path}"
        )
        path = self.create_path(path, date_from)
        self.save_file(path, wb)
        return path

    def parseXML(self, xmlString):
        """
        ЧТЕНИЕ XML С ДАННЫМИ
        :param xmlString: строка XML из интернет-магазина Битрикс
        :return: список словарей с данными
        """
        x = re.search(r' encoding="windows-1251"', xmlString)

        xml = xmlString[: x.start()] + xmlString[x.end() :]
        result = []
        products_in_bay = -1
        last_elem = ""
        with open("xml_root.xml", "w") as f:
            f.write(xml)
        root = objectify.fromstring(xml)
        for doc in root.getchildren():
            paydate = ""
            pay = False
            status = ""
            for req in doc.ЗначенияРеквизитов.getchildren():
                if req.Наименование == "Дата оплаты":
                    paydate = datetime.strftime(
                        datetime.strptime(str(req.Значение), "%d.%m.%Y %H:%M:%S"),
                        "%Y-%m-%d %H:%M:%S",
                    )
                if req.Наименование == "Заказ оплачен":
                    pay = bool(req.Значение)
                if req.Наименование == "Статус заказа":
                    status = str(req.Значение)
            for product in doc.Товары.getchildren():
                count = int(product.Количество)
                while count > 0:
                    count -= 1
                    result.append(dict())
                    if last_elem != doc.Ид:
                        products_in_bay = -1
                    last_elem = doc.Ид
                    products_in_bay += 1
                    result[len(result) - 1]["Id_P"] = int(
                        str(doc.Ид) + str(products_in_bay)
                    )
                    result[len(result) - 1]["OrderNumber"] = int(doc.Ид)
                    result[len(result) - 1]["ProductId"] = str(product.Ид)
                    result[len(result) - 1]["ProductName"] = str(product.Наименование)
                    result[len(result) - 1]["OrderDate"] = datetime.strptime(
                        str(doc.Дата + " " + doc.Время), "%Y-%m-%d %H:%M:%S"
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    result[len(result) - 1]["PayDate"] = paydate
                    result[len(result) - 1]["Sum_P"] = Decimal(
                        float(product.ЦенаЗаЕдиницу)
                    )
                    result[len(result) - 1]["Pay_P"] = pay
                    result[len(result) - 1]["Status_P"] = status
                    result[len(result) - 1]["Client_P"] = str(
                        doc.Контрагенты.Контрагент.Ид
                    )
        if result:
            logging.info(
                f'{datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"):20}:    '
                f"Новый файл. Количество строк - {len(result)}"
            )
        else:
            logging.info(
                f'{datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"):20}:    Нет новых покупок.'
            )
        return result

    def uploadToBase(
        self,
        server,
        database,
        uid,
        pwd,
        Id_P,
        OrderNumber_P,
        ProductId_P,
        ProductName_P,
        OrderDate_P,
        PayDate_P,
        Sum_P,
        Pay_P,
        Status_P,
        Client_P,
    ):
        """
        Отправка данных в sql-базу
        """

        cnxn = get_mssql_connection(server=server, database=database, uid=uid, pwd=pwd)
        cursor = cnxn.cursor()

        cursor.execute(
            f"""
                        INSERT INTO [Transactions](
                            [Id],
                            [OrderNumber],
                            [ProductId],
                            [ProductName],
                            [OrderDate],
                            [PayDate],
                            [Sum],
                            [Pay],
                            [Status],
                            [Client]
                        )
                        VALUES(
                            {Id_P},
                            {OrderNumber_P},
                            '{ProductId_P}',
                            '{ProductName_P}',
                            '{OrderDate_P}',
                            '{PayDate_P}',
                            {Sum_P},
                            {Pay_P},
                            '{Status_P}',
                            '{Client_P}'
                        )
                       """
        )
        cnxn.commit()
        return "Upload To SQL-Base: Ready"

    def if_in_base(
        self,
        server,
        database,
        uid,
        pwd,
        Id_P,
    ):
        cnxn = get_mssql_connection(server=server, database=database, uid=uid, pwd=pwd)
        cursor = cnxn.cursor()

        cursor.execute(
            f"""
                            SELECT
                                [Id],
                                [OrderNumber],
                                [ProductId],
                                [ProductName],
                                [OrderDate],
                                [PayDate],
                                [Sum],
                                [Pay],
                                [Status],
                                [Client]
                            FROM [Transactions]
                            WHERE
                                [Id] = {Id_P}
                           """
        )
        result = []
        while True:
            row = cursor.fetchone()
            if row:
                result.append(row)
            else:
                break
        if len(result) > 0:
            return False
        else:
            return True

    def load_checkbox(self):
        """
        Установка чекбоксов в соответствии с настройками INI-файла
        """
        logging.info(f"{__name__}: {str(datetime.now())[:-7]}:    Загрузка настроек...")
        self.root.ids.report.ids.split_by_days.active = self.split_by_days
        self.root.ids.report.ids.finreport_xls.active = self.finreport_xls
        self.root.ids.report.ids.check_client_count_total_xls.active = (
            self.check_client_count_total_xls
        )
        self.root.ids.report.ids.check_cashreport_xls.active = self.check_cashreport_xls
        self.root.ids.report.ids.check_itogreport_xls.active = self.check_itogreport_xls
        self.root.ids.report.ids.agentreport_xls.active = self.agentreport_xls
        self.root.ids.report.ids.use_yadisk.active = self.use_yadisk
        self.root.ids.report.ids.finreport_google.active = self.finreport_google
        self.root.ids.report.ids.finreport_telegram.active = self.finreport_telegram

    def change_checkbox(self, name, checkbox):
        """
        Изменяет состояние элемента конфигурации и записывает в INI-файл
        :param name: Имя чекбокса
        :param checkbox: Состояние active чекбокса
        """
        self.config.set("General", name, str(checkbox))
        setattr(self, name, checkbox)
        self.config.write()
        logging.info(
            f"{__name__}: {str(datetime.now())[:-7]}:    Параметр {name} изменен на значение {checkbox}"
        )
        if (
            name == "split_by_days"
            and not checkbox
            and not self.root.ids.report.ids.date_switch.active
        ):
            self.root.ids.report.ids.finreport_google.active = False
            self.change_checkbox("finreport_google", False)
            self.root.ids.report.ids.finreport_google.disabled = True
            self.root.ids.report.ids.finreport_google_text.disabled = True
        elif name == "split_by_days" and checkbox:
            self.root.ids.report.ids.finreport_google_text.disabled = False
            self.root.ids.report.ids.finreport_google.disabled = False

    def save_reports(self, date_from):
        """
        Функция управления
        """
        self.fin_report()
        self.agent_report()
        # agentreport_xls
        self.path_list.append(
            self.export_agent_report(self.agentreport_dict, date_from)
        )
        # finreport_google
        self.fin_report_lastyear()
        self.fin_report_beach()
        if self.itog_report_month:
            self.fin_report_month()
            self.agent_report_month()
        self.export_to_google_sheet(date_from)
        # finreport_telegram:
        self.sms_report_list.append(self.sms_report(date_from))
        # check_itogreport_xls:
        if self.itog_report_org1["Итого по отчету"][1]:
            self.path_list.append(
                self.save_organisation_total(self.itog_report_org1, date_from)
            )
        if self.itog_report_org2["Итого по отчету"][1]:
            self.path_list.append(
                self.save_organisation_total(self.itog_report_org2, date_from)
            )
        if self.itog_report_org3["Итого по отчету"][1]:
            self.path_list.append(
                self.save_organisation_total(self.itog_report_org3, date_from)
            )
        if self.itog_report_org4["Итого по отчету"][1]:
            self.path_list.append(
                self.save_organisation_total(self.itog_report_org4, date_from)
            )
        if self.itog_report_org5["Итого по отчету"][1]:
            self.path_list.append(
                self.save_organisation_total(self.itog_report_org5, date_from)
            )
        # check_cashreport_xls:
        if self.cashdesk_report_org1["Итого"][0][1]:
            self.path_list.append(
                self.save_cashdesk_report(self.cashdesk_report_org1, date_from)
            )
        if self.cashdesk_report_org2["Итого"][0][1]:
            self.path_list.append(
                self.save_cashdesk_report(self.cashdesk_report_org2, date_from)
            )
        # check_client_count_total_xls:
        if self.client_count_totals_org1[-1][1]:
            self.path_list.append(
                self.save_client_count_totals(self.client_count_totals_org1, date_from)
            )
        if self.client_count_totals_org2[-1][1]:
            self.path_list.append(
                self.save_client_count_totals(self.client_count_totals_org2, date_from)
            )

    def load_report(self, date_from, date_to):
        """Выполнить отчеты"""

        self.itog_report_org1 = None
        self.itog_report_org2 = None
        self.itog_report_org3 = None
        self.itog_report_org4 = None
        self.itog_report_org5 = None
        self.report_bitrix = None

        self.click_select_org()

        self.report_bitrix = self.read_bitrix_base(
            server=self.server,
            database=self.database_bitrix,
            user=self.user,
            pwd=self.pwd,
            date_from=date_from,
            date_to=date_to,
        )
        self.report_bitrix_lastyear = self.read_bitrix_base(
            server=self.server,
            database=self.database_bitrix,
            user=self.user,
            pwd=self.pwd,
            date_from=date_from - relativedelta(years=1),
            date_to=date_to - relativedelta(years=1),
        )
        self.report_rk = self.rk_report_request(
            server=self.server_rk,
            database=self.database_rk,
            user=self.user_rk,
            pwd=self.pwd_rk,
            cash_id=15033,
            date_from=date_from,
            date_to=date_to,
        )
        self.report_rk_lastyear = self.rk_report_request(
            server=self.server_rk,
            database=self.database_rk,
            user=self.user_rk,
            pwd=self.pwd_rk,
            cash_id=15033,
            date_from=date_from - relativedelta(years=1),
            date_to=date_to - relativedelta(years=1),
        )

        if self.org1:
            self.itog_report_org1 = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org1[0],
                org_name=self.org1[1],
                date_from=date_from,
                date_to=date_to,
            )
            self.itog_report_org1_lastyear = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org1[0],
                org_name=self.org1[1],
                date_from=date_from - relativedelta(years=1),
                date_to=date_to - relativedelta(years=1),
            )
            self.itog_report_org3 = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org3[0],
                org_name=self.org3[1],
                date_from=date_from,
                date_to=date_to,
            )
            self.itog_report_org3_lastyear = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org3[0],
                org_name=self.org3[1],
                date_from=date_from - relativedelta(years=1),
                date_to=date_to - relativedelta(years=1),
            )
            self.itog_report_org4 = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org4[0],
                org_name=self.org4[1],
                date_from=date_from,
                date_to=date_to,
            )
            self.itog_report_org4_lastyear = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org4[0],
                org_name=self.org4[1],
                date_from=date_from - relativedelta(years=1),
                date_to=date_to - relativedelta(years=1),
            )
            self.itog_report_org5 = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org5[0],
                org_name=self.org5[1],
                date_from=date_from,
                date_to=date_to,
            )
            self.itog_report_org5_lastyear = self.itog_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org5[0],
                org_name=self.org5[1],
                date_from=date_from - relativedelta(years=1),
                date_to=date_to - relativedelta(years=1),
            )
            if int((date_to - timedelta(1)).strftime("%y%m")) < int(
                date_to.strftime("%y%m")
            ):
                self.itog_report_month = self.itog_report(
                    server=self.server,
                    database=self.database1,
                    user=self.user,
                    pwd=self.pwd,
                    org=self.org1[0],
                    org_name=self.org1[1],
                    date_from=datetime.strptime(
                        "01" + (date_to - timedelta(1)).strftime("%m%y"), "%d%m%y"
                    ),
                    date_to=date_to,
                )
                self.report_rk_month = self.rk_report_request(
                    server=self.server_rk,
                    database=self.database_rk,
                    user=self.user_rk,
                    pwd=self.pwd_rk,
                    cash_id=15033,
                    date_from=datetime.strptime(
                        "01" + (date_to - timedelta(1)).strftime("%m%y"), "%d%m%y"
                    ),
                    date_to=date_to,
                )
            else:
                self.itog_report_month = None

            self.cashdesk_report_org1 = self.cashdesk_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                date_from=date_from,
                date_to=date_to,
            )
            self.cashdesk_report_org1_lastyear = self.cashdesk_report(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                date_from=date_from - relativedelta(years=1),
                date_to=date_to - relativedelta(years=1),
            )
            self.client_count_totals_org1 = self.client_count_totals_period(
                server=self.server,
                database=self.database1,
                user=self.user,
                pwd=self.pwd,
                org=self.org1[0],
                org_name=self.org1[1],
                date_from=date_from,
                date_to=date_to,
            )
        if self.org2:
            self.itog_report_org2 = self.itog_report(
                server=self.server,
                database=self.database2,
                user=self.user,
                pwd=self.pwd,
                org=self.org2[0],
                org_name=self.org2[1],
                date_from=date_from,
                date_to=date_to,
            )
            self.cashdesk_report_org2 = self.cashdesk_report(
                server=self.server,
                database=self.database2,
                user=self.user,
                pwd=self.pwd,
                date_from=date_from,
                date_to=date_to,
            )
            self.client_count_totals_org2 = self.client_count_totals_period(
                server=self.server,
                database=self.database2,
                user=self.user,
                pwd=self.pwd,
                org=self.org2[0],
                org_name=self.org2[1],
                date_from=date_from,
                date_to=date_to,
            )

        # Чтение XML с привязкой групп услуг к услугам
        self.orgs_dict = self.read_reportgroup(self.reportXML)
        self.itogreport_group_dict = self.read_reportgroup(self.itogreportXML)
        # Поиск новых услуг
        self.find_new_service(self.itog_report_org1, self.orgs_dict)
        self.find_new_service(self.itog_report_org1_lastyear, self.orgs_dict)
        self.find_new_service(self.itog_report_org3, self.orgs_dict)
        self.find_new_service(self.itog_report_org3_lastyear, self.orgs_dict)
        self.find_new_service(self.itog_report_org4, self.orgs_dict)
        self.find_new_service(self.itog_report_org4_lastyear, self.orgs_dict)
        self.find_new_service(self.itog_report_org5, self.orgs_dict)
        self.find_new_service(self.itog_report_org5_lastyear, self.orgs_dict)
        if self.itog_report_month:
            self.find_new_service(self.itog_report_month, self.orgs_dict)

        if self.new_service:
            logger.warning(f"Найдены новые сервисы: {self.new_service}")

        self.agentservice(date_from)

    def run_report(self, date_from, date_to, use_yadisk: bool = False):
        self.path_list = []
        self.sms_report_list = []

        period = []
        while True:
            period.append(date_from)
            if date_from + timedelta(1) == date_to:
                break
            else:
                date_from = date_from + timedelta(1)

        for date in period:
            date_from = date
            date_to = date + timedelta(1)
            self.load_report(date_from, date_to)

        # Отправка в яндекс диск
        if use_yadisk:
            self.path_list = filter(lambda x: x is not None, self.path_list)
            self.sync_to_yadisk(self.path_list, self.yadisk_token, date_from)
            self.path_list = []


def get_legacy_service() -> BarsicReport2Service:
    return BarsicReport2Service()
