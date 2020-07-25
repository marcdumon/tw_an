# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - analytics_facade.py
# md
# --------------------------------------------------------------------------------------------------------

from datetime import datetime

import pandas as pd

from analytics_queries import q_get_nr_tweets_per_day
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve twitter data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------
- get_join_date(username)
- get_a_profile(username)
- get_profiles()
- get_nr_tweets_per_day(username, session_begin_date, session_end_date)
- reset_all_scrape_flags()
- save_a_profile(profiles_df)
- save_tweets(tweets_df)
- set_profile_scrape_flag(username, flag)
"""


def get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    nr_tweets_per_day = q_get_nr_tweets_per_day(username, start_date, end_date)
    return pd.DataFrame(nr_tweets_per_day)


if __name__ == '__main__':
    pass
