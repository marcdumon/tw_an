# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - analytics_facade.py
# md
# --------------------------------------------------------------------------------------------------------

from datetime import datetime

import pandas as pd

from database.analytics_queries import q_get_nr_tweets_per_day, q_get_tweet_datetimes, q_populate_stats, q_delete_all_stats, q_get_stats, q_update_a_year_stat, q_get_tweets
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


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# TOKENS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_tweets(username, begin_date=None, end_date=None):
    begin_date = begin_date if begin_date else datetime.min
    end_date = end_date if end_date else datetime.max
    tweets = q_get_tweets(username, begin_date, end_date)
    tweets_df = pd.DataFrame(tweets)
    return tweets_df


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# STATS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    nr_tweets_per_day = q_get_nr_tweets_per_day(username, start_date, end_date)
    return pd.DataFrame(nr_tweets_per_day)


def get_tweet_datetimes(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    """
    Returns a dataframe with datetime of each tweet.
    """
    tweet_datetimes = q_get_tweet_datetimes(username, begin_date, end_date)
    return pd.DataFrame(tweet_datetimes)



def update_stats(username, freq, stats):
    stats['datetime'] = stats.index.values
    q_update_a_year_stat(username, freq, stats)


def populate_stats(username, freq, stats):
    stats['datetime'] = stats.index.values
    q_populate_stats(username, freq, stats)


def delete_all_stats():
    logger.warning(f'The stats collecion will droped !!!')
    input("Press Enter to continue...")
    q_delete_all_stats()


def get_stats(username, freq):
    stats = q_get_stats(username, freq)
    df = pd.DataFrame()
    for stat in stats:
        df = df.append(pd.DataFrame(stat[f'{freq}']))

    return df


if __name__ == '__main__':
    pass
    delete_all_stats()
