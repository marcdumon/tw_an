# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - proxy_queries.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

from pymongo import MongoClient, DESCENDING
from pymongo.errors import DuplicateKeyError

from config import DATABASE
from tools.logger import logger

"""
Group of queries to store and retrief data from the proxies collection.
The queries start with 'q_' 
Queries accept and return a dict or a lists of dicts when suitable

Convention:
-----------
- documnet:     d
- query:        q
- projection:   p
- sort:         s
- filter:       f
- update:       u
- pipeline      pl
- match         m
- group:        g

IMPLEMENTED QUERIES
-------------------
- q_get_proxies(q)
- q_save_a_proxy(proxy)
- q_update_a_proxy_test(proxy_test)
- q_reset_proxy_stats(proxy)
- q_update_proxy_stats(proxy, flag)
"""

database = DATABASE
collection_name = 'proxies'


def get_collection():  # Todo: same function in many modules. Put in tools?
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():
    collection = get_collection()
    collection.create_index([('ip', DESCENDING), ('port', DESCENDING)], unique=True)


def q_get_proxies(f):
    collection = get_collection()
    # p = {'ip': 1, 'port': 1, 'delay': 1, 'blacklisted': 1, '_id': 0}
    p = None
    cursor = collection.find(f, p)
    proxies = list(cursor)

    return proxies


def q_save_a_proxy(proxy):
    collection = get_collection()
    d = proxy
    # New proxies have not been tested
    d['delay'] = 999999
    d['blacklisted'] = True
    d['error_code'] = 0
    d['test_n_blacklisted'] = 0
    d['test_n_tested'] = 0
    d['scrape_success'] = True
    d['scrape_n_used'] = 0
    d['scrape_n_failed'] = 0
    d['scrape_n_used_total'] = 0
    d['scrape_n_failed_total'] = 0
    try:
        collection.insert_one(d)
    except DuplicateKeyError as e:
        logger.warning(f"Duplicate proxy: {proxy['ip']}:{proxy['port']}")


def q_update_a_proxy_test(proxy_test):
    collection = get_collection()
    f = {'ip': proxy_test['ip'],
         'port': proxy_test['port']}
    u = {'$set': {'delay': proxy_test['delay'],
                  'blacklisted': proxy_test['blacklisted'],
                  'error_code': proxy_test['error_code']},
         '$inc': {'test_n_blacklisted': int(proxy_test['blacklisted']),
                  'test_n_tested': 1}}
    collection.update_one(f, u, upsert=True)


def q_update_proxy_stats(flag, proxy):
    collection = get_collection()
    f = {'ip': proxy['ip'], 'port': proxy['port']}
    u = {'$set': {'last_flag': flag,
                  'timestamp': datetime.now()}}
    if flag == 'ok':
        u['$inc'] = {'scrape_n_used': 1,
                     'scrape_n_used_total': 1,
                     f'flag_stats.{flag}': 1}
    else:
        u['$inc'] = {'scrape_n_used': 1,
                     'scrape_n_used_total': 1,
                     'scrape_n_failed': 1,
                     'scrape_n_failed_total': 1,
                     f'flag_stats.{flag}': 1}
    collection.update_one(f, u, upsert=True)


def q_reset_proxy_stats(proxy, totals=False):
    collection = get_collection()
    f = {'ip': proxy['ip'], 'port': proxy['port']}
    u = {'$set': {'last_flag': '',
                  'scrape_n_used': 0,
                  'scrape_n_failed': 0,
                  'timestamp': datetime.min}}
    if totals: u['$set'].update({'scrape_n_used_total': 0,
                                 'scrape_n_failed_total': 0,
                                 'flag_stats': {},
                                 'fail_ratio': 0})

    collection.update_one(f, u, upsert=True)


def q_update_proxy_ratio(proxy):
    collection = get_collection()
    f = {'ip': proxy['ip'], 'port': proxy['port']}
    u = {'$set': {'fail_ratio': proxy['ratio']}}
    collection.update_one(f, u)


def q_temp():
    collection = get_collection()
    f = {'delay': 0}
    u = {'$set': {'delay': 999999}}
    collection.update_many(f, u, upsert=True)


if __name__ == '__main__':
    pass
