# /usr/bin/python3
# -*- coding: utf-8 -*-

from decimal import Decimal


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
