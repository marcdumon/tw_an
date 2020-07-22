# --------------------------------------------------------------------------------------------------------
# 2020/07/08
# src - proxy_facade.py
# md
# --------------------------------------------------------------------------------------------------------

import pandas as pd

from scraper.database.proxy_queries import q_save_a_proxy, q_get_proxies, q_update_a_proxy_test, \
    q_reset_proxy_stats, q_update_proxy_stats, q_update_proxy_ratio  # , q_update_proxy_stats, q_reset_proxy_stats
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve proxy data via proxy queries from the proxies collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------
- get_proxies(blacklisted=None, max_delay=None)
- save_a_proxy_test(proxy, delay)
- save_proxies(tweets_df)
- set_a_proxy_scrape_success_flag(proxy, flag)
- set_proxies(delay, blacklisted, error_code)
"""


def get_proxies(max_delay=None, max_ratio=None):
    f = {'blacklisted': False, '$and': []}
    if max_delay: f['$and'] = f['$and'] + [{'delay': {'$gt': 0}},
                                           {'delay': {'$lte': max_delay}}]
    if max_ratio: f['$and'] = f['$and'] + [{'fail_ratio': {'$lte': max_ratio}}]
    if not f['$and']: f.pop('$and', None)
    proxies = q_get_proxies(f)
    proxies_df = pd.DataFrame(proxies)
    return proxies_df


def save_a_proxy_test(proxy_test):
    # proxy_test = {'ip': ip, 'port': port, 'delay': delay, 'blacklisted': blacklisted}
    q_update_a_proxy_test(proxy_test)


def save_proxies(proxies_df):
    for _, row in proxies_df.iterrows():
        proxy = row.to_dict()
        q_save_a_proxy(proxy)


def set_proxies(delay=999999, blacklisted=False, error_code=-1):  # Todo: Name is not clear and use update_many in the query iso loop
    for proxy in q_get_proxies({}):
        proxy_test = proxy
        proxy_test['delay'] = delay
        proxy_test['blacklisted'] = blacklisted
        proxy_test['error_code'] = error_code
        q_update_a_proxy_test(proxy_test)


def update_proxy_stats(flag, proxy):
    q_update_proxy_stats(flag, proxy)


def reset_proxies_stats(totals=False):  # todo: Refactor: Use update_multi iso looping
    for proxy in q_get_proxies({}):
        q_reset_proxy_stats(proxy, totals=totals)


def update_proxies_ratio():
    """
    Formula to calculate the ratio's:
    - if scrape_n_used_total < 5 => ratio = 0 # favour new ones
    """
    proxies = q_get_proxies({})
    for proxy in proxies:
        if proxy['scrape_n_used_total'] < 20:
            proxy['ratio'] = 0
        else:
            proxy['ratio'] = proxy['scrape_n_failed_total'] / proxy['scrape_n_used_total']
        q_update_proxy_ratio(proxy)


if __name__ == '__main__':
    pass

