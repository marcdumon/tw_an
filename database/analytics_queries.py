# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - analytics_queries.py
# md
# --------------------------------------------------------------------------------------------------------

from datetime import datetime

from pymongo import MongoClient, ASCENDING

from config import DATABASE

"""
Group of queries to store and retrief data from the tweets collections.
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

"""
database = DATABASE
collection_name = 'analytics_stats'


def get_collection(col=collection_name):  # Todo: Replace get_collection() with get_collection(col=collection_name) everywhere
    client = MongoClient()
    db = client[database]
    collection = db[col]
    return collection


def setup_collection():  # Todo: add indexes
    collection = get_collection()
    collection.create_index('username', unique=True)


x = {
    'username': None,
    'n_tweets_per_day': {},
    'n_tweets_per_week': {},
    'n_tweets_per_month': {},
    'begin_date': None,
    'end_date': None,
    'timestamp': datetime.now()
}


def q_get_nr_tweets_per_day(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    collection = get_collection('tweets')

    m = {'$match': {'username': username,
                    'datetime': {'$gte': begin_date,
                                 '$lte': end_date}}}
    g = {'$group': {'_id': '$date',
                    'nr_tweets': {'$sum': 1}}}
    p = {'$project': {'date': {'$dateFromString': {'dateString': '$_id'}},
                      'nr_tweets': 1, '_id': 0}}
    s = {'$sort': {'date': ASCENDING}}

    cursor = collection.aggregate([m, g, p, s])
    return list(cursor)


if __name__ == '__main__':
    x = q_get_nr_tweets_per_day('franckentheo')
    print(x)
