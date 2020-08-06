# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - analytics_facade.py
# md
# --------------------------------------------------------------------------------------------------------

from datetime import datetime

import pandas as pd

from database.analytics_queries import q_get_tweet_datetimes, q_populate_profile_stats, q_delete_all_stats, q_update_profile_stats, q_get_tweets, q_upsert_tweet_stat
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
# TWEET STATS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def get_tweets(username, begin_date=None, end_date=None):
    begin_date = begin_date if begin_date else datetime.min
    end_date = end_date if end_date else datetime.max
    tweets = q_get_tweets(username, begin_date, end_date)
    tweets_df = pd.DataFrame(tweets)
    return tweets_df


def upsert_tweet_stat(username, year, month, tweet_stats, pos_dict):
    stats = []
    # print(tweet_stats.columns)
    for _, tweet_stat in tweet_stats.iterrows():
        stat = {
            'tweet_id': tweet_stat['tweet_id'],
            'conversation_id': tweet_stat['conversation_id'],
            'datetime': tweet_stat['datetime'],
            'hashtags': tweet_stat['hashtags'],
            'reply_to': tweet_stat['reply_to'],
            'is_reply': tweet_stat['is_reply'],
            'nlikes': tweet_stat['nlikes'],
            'nreplies': tweet_stat['nreplies'],
            'nretweets': tweet_stat['nretweets'],
            'day': tweet_stat['day'],
            'hour': tweet_stat['hour'],
            'len': tweet_stat['len']
        }
        stats.append(stat)
    q_upsert_tweet_stat(username, year, month, stats, pos_dict)


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# PROFILE STATS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def get_tweet_datetimes(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    """
    Returns a dataframe with datetime of each tweet.
    """
    tweet_datetimes = q_get_tweet_datetimes(username, begin_date, end_date)
    return pd.DataFrame(tweet_datetimes)


def update_profile_stats(username, freq, profile_stats):
    profile_stats['datetime'] = profile_stats.index.values
    q_update_profile_stats(username, freq, profile_stats)


def populate_profile_stats(username, freq, profile_stats):
    profile_stats['datetime'] = profile_stats.index.values
    q_populate_profile_stats(username, freq, profile_stats)


def delete_all_stats():
    logger.warning(f'All profile_stats will be deleted !!!')
    input("Press Enter to continue...")
    q_delete_all_stats()


if __name__ == '__main__':
    pass
    delete_all_stats()
    # get_tweets('samvanrooy1')
