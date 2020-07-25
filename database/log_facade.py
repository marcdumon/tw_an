# --------------------------------------------------------------------------------------------------------
# 2020/07/20
# src - log_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

import pandas as pd

from database.log_queries import q_save_log, q_get_max_sesion_id, q_get_dead_tweets_periods_logs

"""
Group of functions to store and retrieve log data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------

"""


def log_scraping_profile(session_id, flag, category, username, **kwargs):
    log = {'session_id': session_id,
           'task': 'profile',
           'category': category,
           'username': username,
           'flag': flag,
           'timestamp': datetime.now()}
    log.update(kwargs)
    q_save_log(log)


def log_scraping_tweets(session_id, flag, category, username, begin_date, end_date, **kwargs):
    log = {'session_id': session_id,
           'task': 'tweets',
           'category': category,
           'username': username,
           'session_begin_date': datetime.combine(begin_date, datetime.min.time()),
           'session_end_date': datetime.combine(end_date, datetime.min.time()),
           'flag': flag,
           'timestamp': datetime.now()}
    log.update(kwargs)
    q_save_log(log)


def get_dead_tweets_periods(session_id=-1):
    session_id = q_get_max_sesion_id() if session_id == -1 else session_id
    print(session_id)
    failed_periods_logs = q_get_dead_tweets_periods_logs(session_id)
    if failed_periods_logs:
        deads_df = pd.DataFrame(failed_periods_logs)
        deads_df['session_begin_date'] = deads_df['session_begin_date'].dt.date
        deads_df['session_end_date'] = deads_df['session_end_date'].dt.date
        return deads_df
    else:
        return pd.DataFrame()


def get_max_sesion_id():
    return q_get_max_sesion_id()


if __name__ == '__main__':
    pass
