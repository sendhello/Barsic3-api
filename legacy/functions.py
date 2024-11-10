from copy import deepcopy

from api.v1.report_settings import logger


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
