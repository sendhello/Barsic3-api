# /usr/bin/python3
# -*- coding: utf-8 -*-

from decimal import Decimal
from datetime import datetime, timedelta


def convert_to_dict(func):
    def itog_report_convert_to_dict(*args, **kwargs):
        """
        Преобразует список кортежей отчета в словарь
        :param report: list - Итоговый отчет в формате списка картежей полученный из функции full_report
        :return: dict - Словарь услуг и их значений
        """
        report = func(*args, **kwargs)
        result = {}
        for row in report:
            result[row[4]] = (row[1], row[0], row[6], row[7])
        return result
    return itog_report_convert_to_dict


def add_sum(func):
    def add_sum_wrapper(*args, **kwargs):
        """
        Расчитывает и добавляет к словарю-отчету 1 элемент: Итого
        :param report: dict - словарь-отчет
        :return: dict - словарь-отчет
        """
        report = func(*args, **kwargs)
        sum_service = Decimal(0)
        sum_many = Decimal(0)
        for line in report:
            if not (report[line][0] is None or report[line][0] is None):
                if line != 'Депозит':
                    sum_service += report[line][0]
                sum_many += report[line][1]
        report['Итого по отчету'] = (sum_service, sum_many, '', 'Итого по отчету')
        return report
    return add_sum_wrapper

def add_date(func):
    def add_sum_wrapper(
            self,
            server,
            database,
            driver,
            user,
            pwd,
            org,
            org_name,
            date_from,
            date_to,
            hide_zeroes='0',
            hide_internal='1',
    ):
        """
        Добавляет к словарю-отчету 1 элемент: Дата
        :param report: dict - словарь-отчет
        :return: dict - словарь-отчет
        """
        report = func(
            self,
            server,
            database,
            driver,
            user,
            pwd,
            org,
            org_name,
            date_from,
            date_to,
            hide_zeroes='0',
            hide_internal='1',
        )
        report['Дата'] = (date_from, date_to, '', '')
        return report
    return add_sum_wrapper


def to_googleshet(func):
    def decimal_to_googlesheet(*args, **kwargs):
        """
        Преобразует суммы Decimal в float
        """
        dict = func(*args, **kwargs)
        new_dict = {}
        for key in dict:
            if type(dict[key][0]) is Decimal:
                new_dict[key] = (int(dict[key][0]), float(dict[key][1]), dict[key][2], dict[key][3])
            else:
                new_dict[key] = (dict[key][0], dict[key][1], dict[key][2], dict[key][3])
        return new_dict
    return decimal_to_googlesheet


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
    return {"red": int(htmlColor[0:2], 16) / 255.0, "green": int(htmlColor[2:4], 16) / 255.0,
            "blue": int(htmlColor[4:6], 16) / 255.0}

def to_bool(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        return None