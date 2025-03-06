from copy import deepcopy
from decimal import Decimal

from api.v1.report_settings import logger
from schemas.rk import SmileReport


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


def concatenate_total_reports(*reports: dict[str, tuple]) -> dict[str, tuple]:
    if len(reports) == 0:
        return {}

    if len(reports) == 1:
        return reports[0]

    result_report = deepcopy(reports[0])
    for another_report in reports[1:]:
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
    smile_report_month: SmileReport,
):
    """Создает отчет платежного агента за месяц."""

    result = {}
    result["Контрольная сумма"] = {}
    result["Контрольная сумма"]["Cумма"] = [["Сумма", 0, 0.0]]
    result["ИТОГО"] = {}
    result["ИТОГО"][""] = [["Сумма", 0, 0.0]]

    for org in agent_dict:
        result[org] = {}
        result[org]["Итого по группе"] = [["Итого по группе", 0, 0.0]]
        for tariff in agent_dict[org]:
            try:
                service_name = month_total_report[tariff][2]
                count = month_total_report[tariff][0]
                summ = month_total_report[tariff][1]

                if tariff == "Дата":
                    result[org][tariff] = []
                    result[org][tariff].append(
                        [
                            tariff,
                            count,
                            summ,
                        ]
                    )
                elif tariff == "Депозит":
                    result[org][tariff] = []
                    result[org][tariff].append([tariff, 0, summ])
                    result[org]["Итого по группе"][0][2] += summ
                    result["Контрольная сумма"]["Cумма"][0][2] += summ
                elif tariff == "Организация":
                    pass
                else:
                    try:
                        if result[org][service_name]:
                            result[org][service_name].append(
                                [
                                    tariff,
                                    count,
                                    summ,
                                ]
                            )
                            result[org][service_name][0][1] += count
                            result[org][service_name][0][2] += summ
                            result[org]["Итого по группе"][0][1] += count
                            result[org]["Итого по группе"][0][2] += summ
                            if tariff != "Итого по отчету":
                                result["Контрольная сумма"]["Cумма"][0][1] += count
                                result["Контрольная сумма"]["Cумма"][0][2] += summ
                        else:
                            result[org][service_name] = []
                            result[org][service_name].append(["Итого по папке", 0, 0.0])
                            result[org][service_name].append(
                                [
                                    tariff,
                                    count,
                                    summ,
                                ]
                            )
                            result[org][service_name][0][1] += count
                            result[org][service_name][0][2] += summ
                            result[org]["Итого по группе"][0][1] += count
                            result[org]["Итого по группе"][0][2] += summ
                            if tariff != "Итого по отчету":
                                result["Контрольная сумма"]["Cумма"][0][1] += count
                                result["Контрольная сумма"]["Cумма"][0][2] += summ
                    except KeyError:
                        result[org][service_name] = []
                        result[org][service_name].append(["Итого по папке", 0, 0.0])
                        result[org][service_name].append(
                            (
                                tariff,
                                count,
                                summ,
                            )
                        )
                        result[org][service_name][0][1] += count
                        result[org][service_name][0][2] += summ
                        result[org]["Итого по группе"][0][1] += count
                        result[org]["Итого по группе"][0][2] += summ
                        if tariff != "Итого по отчету":
                            result["Контрольная сумма"]["Cумма"][0][1] += count
                            result["Контрольная сумма"]["Cумма"][0][2] += summ
            except KeyError:
                pass
            except TypeError:
                pass

            if tariff == "Смайл":
                result[org]["Смайл"] = []
                result[org]["Смайл"].append(["Итого по папке", 0, 0.0])
                result[org]["Смайл"].append(
                    (
                        "Смайл",
                        smile_report_month.total_count,
                        smile_report_month.total_sum,
                    )
                )
                result[org]["Смайл"][0][1] += smile_report_month.total_count
                result[org]["Смайл"][0][2] += smile_report_month.total_sum
                result[org]["Итого по группе"][0][1] += smile_report_month.total_count
                result[org]["Итого по группе"][0][2] += smile_report_month.total_sum
                result["Контрольная сумма"]["Cумма"][0][
                    1
                ] += smile_report_month.total_count
                result["Контрольная сумма"]["Cумма"][0][
                    2
                ] += smile_report_month.total_sum
                result["ИТОГО"][""][0][1] += smile_report_month.total_count
                result["ИТОГО"][""][0][2] += smile_report_month.total_sum

    if (
        result["ИТОГО"][""][0][2] != result["Контрольная сумма"]["Cумма"][0][2]
        or result["ИТОГО"][""][0][1] != result["Контрольная сумма"]["Cумма"][0][1]
    ):
        logger.error(
            f"Несоответствие Контрольных сумм. "
            f"Итого по отчету ({result['ИТОГО'][''][0][1]}: "
            f"{result['ИТОГО'][''][0][2]}) не равно Контрольной сумме услуг"
            f"({result['Контрольная сумма']['Cумма'][0][1]}: "
            f"{result['Контрольная сумма']['Cумма'][0][2]})"
        )

    return result


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
    org,
    org_name,
    date_from,
    date_to,
    hide_zeroes="0",
    hide_internal="1",
    hide_discount="0",
    is_legacy_database=False,
):
    """Делает запрос в базу Барс и возвращает итоговый отчет за запрашиваемый период."""

    date_from_date = date_from.strftime("%Y%m%d 00:00:00")
    date_to_date = date_to.strftime("%Y%m%d 00:00:00")

    SQL_REQUEST = (
        f"exec sp_reportOrganizationTotals_v2 "
        f"@sa={org},"
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
    result[org_name] = (0, 0, "Организация", "Организация")
    result[str(org)] = (0, 0, "ID организации", "ID организации")

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
