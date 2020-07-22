# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - utils.py
# md
# --------------------------------------------------------------------------------------------------------
import datetime
import urllib
from urllib import parse

import pandas as pd


def current_datetime(alt: str = False) -> [datetime.datetime, str]:
    """Returns current datetime object by default. Accepts alternate format for string format result."""
    if alt:
        return datetime.datetime.now().strftime(alt)
    return datetime.datetime.now()


def current_date(alt: str = False) -> [datetime.date, str]:
    """Returns current date object by default. Accepts alternate format for string format result."""
    if alt:
        return datetime.date.today().strftime(alt)
    return datetime.date.today()


def str2d(date_time_str):
    """Convert a 'yyyy-mm-dd' to datetime(yyyy, mm, dd)"""
    return datetime.datetime.strptime(date_time_str, '%Y-%m-%d')


def dt2str(dt):
    """Convert a 'yyyy-mm-dd' to datetime(yyyy, mm, dd)"""
    return dt.strftime('%Y-%m-%d')


def dict_to_query(dictionary: dict) -> str:
    return urllib.parse.urlencode(dictionary)


def set_pandas_display_options():
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


if __name__ == '__main__':
    print(current_datetime('%Y:%m:%d'))
    print(dict_to_query({'a': 1, 'b': current_date()}))
