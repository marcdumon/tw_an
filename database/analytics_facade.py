# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - analytics_facade.py
# md
# --------------------------------------------------------------------------------------------------------

from datetime import datetime

import pandas as pd

from database.analytics_queries import q_get_nr_tweets_per_day, q_get_tweet_datetimes, q_populate_stats, q_delete_all_stats
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve analytics data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------
- get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1))
- def get_tweet_datetimes(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1))

-

"""


def get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    nr_tweets_per_day = q_get_nr_tweets_per_day(username, start_date, end_date)
    return pd.DataFrame(nr_tweets_per_day)


def get_tweet_datetimes(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    """
    Returns a dataframe with datetime of each tweet.
    """
    tweet_datetimes = q_get_tweet_datetimes(username, begin_date, end_date)
    return pd.DataFrame(tweet_datetimes)


def update_stats(username, stats, freq):
    stats['datetime'] = stats.index.values
    # q_update_a_year_stat(username, freq, stats)
    q_populate_stats(username, freq, stats)


def populate_stats(username, stats, freq):
    pass


def delete_all_stats():
    logger.warning(f'The stats collecion will droped !!!')
    input("Press Enter to continue...")
    q_delete_all_stats()


if __name__ == '__main__':
    pass
