from copy import deepcopy
from decimal import Decimal

from api.v1.report_settings import logger
from schemas.rk import SmileReport
from sql.clients_count import CLIENTS_COUNT_SQL
from schemas.bars import ClientsCount, Organisation


def is_int(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def func_pass():
    pass


def htmlColorToJSON(htmlColor):
    if htmlColor.startswith("#"):
        htmlColor = htmlColor[1:]
    return {
        "red": int(htmlColor[0:2], 16) / 255.0,
        "green": int(htmlColor[2:4], 16) / 255.0,
        "blue": int(htmlColor[4:6], 16) / 255.0,
    }


def to_bool(s):
    if s == "True":
        return True
    elif s == "False":
        return False
    else:
        return None


def concatenate_itog_reports(*itog_reports: dict[str, tuple]):
    if len(itog_reports) == 0:
        return {}

    if len(itog_reports) == 1:
        return itog_reports[0]

    result_report = deepcopy(itog_reports[0])
    for another_report in itog_reports[1:]:
        for position, values in another_report.items():
            if position in result_report and position != "Дата":
                result_report[position] = (
                    sum([result_report[position][0], another_report[position][0] or 0]),
                    sum([result_report[position][1], another_report[position][1] or 0]),
                    result_report[position][2],
                    result_report[position][3],
                )
            else:
                result_report[position] = another_report[position]

    return result_report


def create_month_agent_report(
    month_total_report: dict[str, tuple],
    agent_dict: dict,
):
    """Создает отчет платежного агента за месяц."""

    month_agent_report = {}
    month_agent_report["Контрольная сумма"] = {}
    month_agent_report["Контрольная сумма"]["Cумма"] = [["Сумма", 0, 0.0]]
    for org in agent_dict:
        month_agent_report[org] = {}
        month_agent_report[org]["Итого по группе"] = [["Итого по группе", 0, 0.0]]
        for tariff in agent_dict[org]:
            try:
                if tariff == "Дата":
                    month_agent_report[org][tariff] = []
                    month_agent_report[org][tariff].append(
                        [
                            tariff,
                            month_total_report[tariff][0],
                            month_total_report[tariff][1],
                        ]
                    )
                elif tariff == "Депозит":
                    month_agent_report[org][tariff] = []
                    month_agent_report[org][tariff].append(
                        [tariff, 0, month_total_report[tariff][1]]
                    )
                    month_agent_report[org]["Итого по группе"][0][
                        2
                    ] += month_total_report[tariff][1]
                    month_agent_report["Контрольная сумма"]["Cумма"][0][
                        2
                    ] += month_total_report[tariff][1]
                elif tariff == "Организация":
                    pass
                else:
                    try:
                        if month_agent_report[org][month_total_report[tariff][2]]:
                            month_agent_report[org][
                                month_total_report[tariff][2]
                            ].append(
                                [
                                    tariff,
                                    month_total_report[tariff][0],
                                    month_total_report[tariff][1],
                                ]
                            )
                            month_agent_report[org][month_total_report[tariff][2]][0][
                                1
                            ] += month_total_report[tariff][0]
                            month_agent_report[org][month_total_report[tariff][2]][0][
                                2
                            ] += month_total_report[tariff][1]
                            month_agent_report[org]["Итого по группе"][0][
                                1
                            ] += month_total_report[tariff][0]
                            month_agent_report[org]["Итого по группе"][0][
                                2
                            ] += month_total_report[tariff][1]
                            if tariff != "Итого по отчету":
                                month_agent_report["Контрольная сумма"]["Cумма"][0][
                                    1
                                ] += month_total_report[tariff][0]
                                month_agent_report["Контрольная сумма"]["Cумма"][0][
                                    2
                                ] += month_total_report[tariff][1]
                        else:
                            month_agent_report[org][month_total_report[tariff][2]] = []
                            month_agent_report[org][
                                month_total_report[tariff][2]
                            ].append(["Итого по папке", 0, 0.0])
                            month_agent_report[org][
                                month_total_report[tariff][2]
                            ].append(
                                [
                                    tariff,
                                    month_total_report[tariff][0],
                                    month_total_report[tariff][1],
                                ]
                            )
                            month_agent_report[org][month_total_report[tariff][2]][0][
                                1
                            ] += month_total_report[tariff][0]
                            month_agent_report[org][month_total_report[tariff][2]][0][
                                2
                            ] += month_total_report[tariff][1]
                            month_agent_report[org]["Итого по группе"][0][
                                1
                            ] += month_total_report[tariff][0]
                            month_agent_report[org]["Итого по группе"][0][
                                2
                            ] += month_total_report[tariff][1]
                            if tariff != "Итого по отчету":
                                month_agent_report["Контрольная сумма"]["Cумма"][0][
                                    1
                                ] += month_total_report[tariff][0]
                                month_agent_report["Контрольная сумма"]["Cумма"][0][
                                    2
                                ] += month_total_report[tariff][1]
                    except KeyError:
                        month_agent_report[org][month_total_report[tariff][2]] = []
                        month_agent_report[org][month_total_report[tariff][2]].append(
                            ["Итого по папке", 0, 0.0]
                        )
                        month_agent_report[org][month_total_report[tariff][2]].append(
                            (
                                tariff,
                                month_total_report[tariff][0],
                                month_total_report[tariff][1],
                            )
                        )
                        month_agent_report[org][month_total_report[tariff][2]][0][
                            1
                        ] += month_total_report[tariff][0]
                        month_agent_report[org][month_total_report[tariff][2]][0][
                            2
                        ] += month_total_report[tariff][1]
                        month_agent_report[org]["Итого по группе"][0][
                            1
                        ] += month_total_report[tariff][0]
                        month_agent_report[org]["Итого по группе"][0][
                            2
                        ] += month_total_report[tariff][1]
                        if tariff != "Итого по отчету":
                            month_agent_report["Контрольная сумма"]["Cумма"][0][
                                1
                            ] += month_total_report[tariff][0]
                            month_agent_report["Контрольная сумма"]["Cумма"][0][
                                2
                            ] += month_total_report[tariff][1]
            except KeyError:
                pass
            except TypeError:
                pass
    if (
        month_agent_report["ИТОГО"][""][1][2]
        != month_agent_report["Контрольная сумма"]["Cумма"][0][2]
        or month_agent_report["ИТОГО"][""][1][1]
        != month_agent_report["Контрольная сумма"]["Cумма"][0][1]
    ):
        logger.error(
            f"Несоответствие Контрольных сумм. "
            f"Итого по отчету ({month_agent_report['ИТОГО'][''][1][1]}: "
            f"{month_agent_report['ИТОГО'][''][1][2]}) не равно Контрольной сумме услуг"
            f"({month_agent_report['Контрольная сумма']['Cумма'][0][1]}: "
            f"{month_agent_report['Контрольная сумма']['Cумма'][0][2]})"
        )

    return month_agent_report


def create_month_finance_report(
    itog_report_month: dict[str, tuple],
    itogreport_group_dict: dict,
    orgs_dict: dict,
    smile_report_month: SmileReport,
):
    """Создает финансовый отчет за месяц."""

    month_finance_report = {}
    control_sum_group = month_finance_report.setdefault("Контрольная сумма", {})
    control_sum = control_sum_group.setdefault("Cумма", [["Сумма", 0, 0.0]])

    for group_name, groups in itogreport_group_dict.items():
        finreport_group = month_finance_report.setdefault(group_name, {})
        finreport_group_total = finreport_group.setdefault(
            "Итого по группе", [["Итого по группе", 0, 0.0]]
        )
        for oldgroup in groups:
            try:
                for service_name in orgs_dict[oldgroup]:
                    try:
                        service_count, service_sum, org_name, group_name = (
                            itog_report_month[service_name]
                        )

                        if service_name == "Дата":
                            product_group = finreport_group.setdefault(oldgroup, [])
                            product_group.append(
                                [service_name, service_count, service_sum]
                            )
                        elif service_name == "Депозит":
                            product_group = finreport_group.setdefault(oldgroup, [])
                            product_group.append([service_name, 0, service_sum])
                            finreport_group_total[0][2] += service_sum
                            control_sum[0][2] += service_sum
                        elif service_name == "Организация":
                            pass
                        else:
                            product_group = finreport_group.setdefault(
                                org_name, [["Итого по папке", 0, 0.0]]
                            )
                            product_group.append(
                                [service_name, service_count, service_sum]
                            )
                            product_group[0][1] += service_count
                            product_group[0][2] += service_sum
                            finreport_group_total[0][1] += service_count
                            finreport_group_total[0][2] += service_sum
                            if service_name != "Итого по отчету":
                                control_sum[0][1] += service_count
                                control_sum[0][2] += service_sum
                    except KeyError:
                        continue
                    except TypeError:
                        continue

            except KeyError as e:
                logger.error(
                    f"Несоответствие конфигураций XML-файлов\n"
                    f"Группа {oldgroup} не существует! \nKeyError: {e}"
                )

            if oldgroup == "Общепит":
                product_group = finreport_group.setdefault(
                    "Общепит (Смайл)",
                    [["Итого по папке", 0, 0.0]],
                )
                product_group.append(
                    [
                        "Смайл",
                        smile_report_month.total_count,
                        smile_report_month.total_sum,
                    ]
                )
                product_group[0][1] += smile_report_month.total_count
                product_group[0][2] += smile_report_month.total_sum
                finreport_group_total[0][1] += smile_report_month.total_count
                finreport_group_total[0][2] += smile_report_month.total_sum

    control_sum[0][1] += smile_report_month.total_count
    control_sum[0][2] += smile_report_month.total_sum
    month_finance_report["ИТОГО"]["Итого по группе"][0][
        1
    ] += smile_report_month.total_count
    month_finance_report["ИТОГО"]["Итого по группе"][0][
        2
    ] += smile_report_month.total_sum
    month_finance_report["ИТОГО"][""][0][1] += smile_report_month.total_count
    month_finance_report["ИТОГО"][""][0][2] += smile_report_month.total_sum
    month_finance_report["ИТОГО"][""][1][1] += smile_report_month.total_count
    month_finance_report["ИТОГО"][""][1][2] += smile_report_month.total_sum
    if (
        month_finance_report["ИТОГО"][""][1][2] != control_sum[0][2]
        or month_finance_report["ИТОГО"][""][1][1] != control_sum[0][1]
    ):
        logger.error(
            f"Несоответствие Контрольных сумм. "
            f"Итого по отчету ({month_finance_report['ИТОГО'][''][1][1]}: "
            f"{month_finance_report['ИТОГО'][''][1][2]}) не равно Контрольной сумме услуг"
            f"({control_sum[0][1]}: {control_sum[0][2]})"
        )

    return month_finance_report


def get_total_report(
    connect,
    company: Organisation,
    date_from,
    date_to,
    hide_zeroes="0",
    hide_internal="1",
    hide_discount="0",
    is_legacy_database=False,
) -> dict:
    """Делает запрос в базу Барс и возвращает итоговый отчет за запрашиваемый период."""

    date_from_date = date_from.strftime("%Y%m%d 00:00:00")
    date_to_date = date_to.strftime("%Y%m%d 00:00:00")

    SQL_REQUEST = (
        f"exec sp_reportOrganizationTotals_v2 "
        f"@sa={company.super_account_id},"
        f"@from='{date_from_date}',"
        f"@to='{date_to_date}',"
        f"@hideZeroes={hide_zeroes},"
        f"@hideInternal={hide_internal}"
    )
    # В аквапарке новая версия БД, добавляем новое поле в запрос
    if not is_legacy_database:
        SQL_REQUEST += f",@hideDiscount={hide_discount}"

    cursor = connect.cursor()
    cursor.execute(SQL_REQUEST)
    rows = cursor.fetchall()

    result = {
        row[4]: (
            int(row[1]) if isinstance(row[1], Decimal) else row[1],
            float(row[0]) if isinstance(row[0], Decimal) else row[0],
            row[6],
            row[7],
        )
        for row in rows
    }
    result[company.descr] = (0, 0, "Организация", "Организация")
    result[str(company.super_account_id)] = (0, 0, "ID организации", "ID организации")

    # добавление строки "Итого по отчету"
    sum_service = 0
    sum_many = 0
    for line in result:
        if not (result[line][0] is None or result[line][0] is None):
            if line != "Депозит":
                sum_service += result[line][0]
            sum_many += result[line][1]
    result["Итого по отчету"] = (sum_service, sum_many, "", "Итого по отчету")

    # добавление даты
    result["Дата"] = (date_from, date_to, "", "")

    return result


def get_clients_count(connect) -> list[ClientsCount]:
    """Получение количества человек в зоне."""

    cursor = connect.cursor()
    cursor.execute(CLIENTS_COUNT_SQL)
    rows = cursor.fetchall()
    if not rows:
        return [ClientsCount(count=0, id=488, zone_name="", code="0003")]

    return [
        ClientsCount(count=row[0], id=row[1], zone_name=row[2], code=row[3])
        for row in rows
    ]
