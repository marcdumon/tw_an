# --------------------------------------------------------------------------------------------------------
# 2020/07/22
# src - test.py
# md
# --------------------------------------------------------------------------------------------------------

import pandas as pd
from pymongo import MongoClient

database = 'test_full'


def get_collection(collection_name):  # Todo: same function in many modules. Put in tools?
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


tweets = get_collection('tweets')
profiles = get_collection('profiles')


def get_tweets_user_day():
    pl = [{
        '$group': {
            '_id': {'username': '$username', 'date': '$date'},
            'count': {'$sum': 1}}},
        {'$sort': {'_id.username': 1, '_id.date': 1}},
        {'$project': {
            '_id': 0,
            'username': '$_id.username',
            'date': '$_id.date',
            'count': 1}}
    ]
    result = tweets.aggregate(pl, allowDiskUse=True)
    result = list(result)
    return pd.DataFrame(result, columns=['username', 'date', 'count'])


if __name__ == '__main__':
    get_tweets_user_day()
