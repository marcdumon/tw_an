# --------------------------------------------------------------------------------------------------------
# 2020/07/20
# src - log_queries.py
# md
# --------------------------------------------------------------------------------------------------------


"""
Group of queries to store and retrief log data.
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
- q_save_log(log)
-
"""

from pymongo import MongoClient

from config import DATABASE

database = DATABASE
collection_name = 'logs'


def get_collection():  # Todo: same function in many modules. Put in tools?
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():
    collection = get_collection()
    # collection.create_index([('ip', DESCENDING), ('port', DESCENDING)], unique=True)


def q_save_log(log):
    collection = get_collection()
    collection.insert_one(log)


def q_get_max_sesion_id():
    collection = get_collection()
    cursor = collection.find({}, {'_id': 0, 'session_id': 1}).sort([('session_id', -1), ('username', 1)]).limit(1)
    doc = list(cursor)
    if doc:
        max_session_id = doc[0]['session_id']
    else:
        max_session_id = -1
    return max_session_id


def q_get_dead_tweets_periods_logs(session_id):
    collection = get_collection()
    f = {'session_id': session_id,
         'task': 'tweets',
         'category': 'period',
         'flag': 'dead'}
    p = {'_id': 0,
         'username': 1,
         'session_begin_date': 1,
         'session_end_date': 1}
    cursor = collection.find(f, p).sort([('username', 1), ('start_period', 1)])
    return list(cursor)


if __name__ == '__main__':
    pass
