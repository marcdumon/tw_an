# --------------------------------------------------------------------------------------------------------
# 2020/07/25
# src - analytics_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
import time
from datetime import datetime

from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import DuplicateKeyError

from config import DATABASE
from tools.logger import logger

"""
Group of queries to store and retrief data from the stats collections.
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
stat_collection_name = 'twitter_stats'
# token_collection_name = 'tokens_stats'
tweets_collection_name = 'tweets'


def get_collection(col=stat_collection_name):  # Todo: Replace get_collection() with get_collection(col=collection_name) everywhere
    client = MongoClient()
    db = client[database]
    collection = db[col]

    return collection


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# TOKENS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def q_get_tweets(username, begin_date, end_date):
    collection = get_collection(tweets_collection_name)
    f = {
        'username': username,
        'datetime': {
            '$gte': begin_date,
            '$lte': end_date,
        }
    }
    p = {
        '_id': 0,
        'username': 1,
        'tweet_id': 1,
        'datetime': 1,
        'tweet': 1,
        'hashtags': 1
    }
    result = collection.find(f, p)
    return list(result)


def upsert_tokens():
    pass
    d = {
        'username': '',
        'year': 0,
        'month': 0,
        'metadata': [{
            'tweet_id': '',
            'datetime': None,
            'metadata': [],
            'emojis': [],
            'hashtags': [],
            'n_tokens': 0,
            'n_emojis': 0,
            'n_hashtags': 0
        }]
    }


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# STATS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def setup_stats_collection():
    collection = get_collection()
    collection.create_index([('username', 1), ('year', 1)], unique=True)


def q_get_nr_tweets_per_day(username, begin_date, end_date):
    collection = get_collection('tweets')

    m = {
        '$match': {
            'username': username,
            'datetime': {
                '$gte': begin_date,
                '$lte': end_date
            }
        }
    }
    g = {'$group': {'_id': '$date',
                    'n_tweets': {'$sum': 1}}}
    p = {'$project': {'date': {'$dateFromString': {'dateString': '$_id'}},
                      'n_tweets': 1, '_id': 0}}
    s = {'$sort': {'date': ASCENDING}}

    result = collection.aggregate([m, g, p, s])
    return list(result)


def q_get_tweet_datetimes(username, begin_date, end_date):
    collection = get_collection('tweets')
    f = {
        'username': username,
        'datetime': {'$gte': begin_date,
                     '$lte': end_date}
    }
    p = {'datetime': 1, '_id': 0}
    result = collection.find(f, p)
    return list(result)


def q_get_a_stat(username, freq):
    collection = get_collection(stat_collection_name)
    f = {'username': username, 'freq': freq}
    result = collection.find_one()
    return list(result) if result else []


def q_bulk_write_a_stat(requests):
    collection = get_collection(stat_collection_name)
    result = collection.bulk_write(requests)
    logger.debug(f'Bulk result | {result.bulk_api_result}')


def q_insert_a_stat(username, freq, stat):
    collection = get_collection(stat_collection_name)
    d = {
        'username': username,
        'year': stat['datetime'].year,
        'timestamp': datetime.now(),
        f'{freq}': [{
            'datetime': stat['datetime'].to_pydatetime(),  # Todo: probably not necessary
            'sum': stat['sum'],
            'max': stat['max'],
            'mean': stat['mean'],
            'cumsum': stat['cumsum']
        }]
    }
    result = collection.insert_one(d)
    return result


def q_upsert_a_stat(username, freq, stat):
    collection = get_collection(stat_collection_name)
    f = {
        'username': username,
        'year': stat['datetime'].year
    }
    u = {
        '$set': {
            'username': username,
            'year': stat['datetime'].year,
            'timestamp': datetime.now(),
        },
        '$push': {
            f'{freq}': {
                'datetime': stat['datetime'].to_pydatetime(),
                'sum': stat['sum'],
                'max': stat['max'],
                'mean': stat['mean'],
                'cumsum': stat['cumsum']}
        }
    }
    result = collection.update_one(f, u, upsert=True)
    return result


def q_update_a_stat(username, freq, stat):
    collection = get_collection(stat_collection_name)
    f = {
        'username': username,
        'year': stat['datetime'].year,
        f'{freq}.datetime': stat['datetime']
    }
    u = {
        '$set': {
            'timestamp': datetime.now(),
            f'{freq}.$.datetime': stat['datetime'],
            f'{freq}.$.sum': stat['sum'],
            f'{freq}.$.max': stat['max'],
            f'{freq}.$.mean': stat['mean'],
            f'{freq}.$.cumsum': stat['cumsum']
        }
    }
    time.sleep(.005)  # Toso: Check is sleep is necessary for update to work
    result = collection.update_one(f, u, False, True)
    return result


def q_delete_all_stats():
    collection = get_collection(stat_collection_name)
    collection.delete_many({})


def q_update_a_year_stat(username, freq, stats):
    # Todo: This method is a bit hacky and could probably be improved.
    #       The method receives a list of stats-dicts for  a certain freq. It then tries to update
    #       the stat in the array f'{freq}'. It checks it the datetime matches. If true then it uses the position operator $
    #       to find the index of datetime in f'{freq}'.datetime, and uses that index to update the stat.
    #       If the new stat has a different value then it gets updated and result.modified_count is not 0.
    #       If the new stat is exactly the same as the stored one, the it doesn't get updated. The timestamp field ensures that even in
    #       that case, the stat will be updated, otherwise the same stat will be pushed.
    #       If the stat does'n texist, result.modified_count is 0 and the stat will be pushed. We can't use upsert here because positionqal operator doesn't work with
    #       upsert.
    # Inspiration: http://blog.rcard.in/database/mongodb/time-series/2017/01/31/implementing-time-series-in-mongodb.html
    collection = get_collection(stat_collection_name)
    for stat in stats.to_dict('records'):
        result = q_update_a_stat(username, freq, stat)
        if not result.modified_count:  # No update means that the document or stat doesn't exists
            try:
                q_upsert_a_stat(username, freq, stat)
            except DuplicateKeyError:  # For when inserting iso upserting.
                logger.debug(f'DuplicateKeyError | {username}, {freq}, {stat}')
            except:
                print(sys.exc_info())
                raise


def q_populate_stats(username, freq, stats):
    """
    todo: This only works with a unique index on username+year
    """
    logger.warning('This only works with a unique index on username+year!')
    collection = get_collection(stat_collection_name)
    dts = stats['datetime'].to_list()
    years = sorted(set([dt.year for dt in dts]))
    for year in years:
        year_stat = stats.loc[datetime(year, 1, 1, 0, 0, 0):datetime(year, 12, 31, 23, 59, 59)]

        # Try to insert new document with empty stats. Make sure there is a unique index on username+year !
        d = {
            'username': username,
            'year': year,
            'timestamp': datetime.now()
        }
        try:
            collection.insert_one(d)

        except DuplicateKeyError:
            # We have an empty stat for username and year so every doc will raise DuplicateKeyError
            pass
        except:
            print(sys.exc_info())
            raise
        finally:
            pass
        # Now are sure that the empty doc exist. Lets bulk update it with stats
        requests = []
        for stat in year_stat.to_dict('records'):
            f = {
                'username': username,
                'year': stat['datetime'].year,
            }
            u = {
                '$set': {
                    # 'username': username,
                    # 'year': stat['datetime'].year,
                    'timestamp': datetime.now(),
                },
                '$push': {
                    f'{freq}': {
                        'datetime': stat['datetime'].to_pydatetime(),
                        'sum': stat['sum'],
                        'max': stat['max'],
                        'mean': stat['mean'],
                        'cumsum': stat['cumsum']}
                }
            }
            requests.append(UpdateOne(f, u, True))
        q_bulk_write_a_stat(requests)


def q_get_stats(username, freq):
    collection = get_collection(stat_collection_name)
    f = {
        'username': username,
        f'{freq}': {'$exists': True}
    }
    p = {
        'username': 1,
        f'{freq}': 1,
        '_id': 0
    }
    result = collection.find(f, p)

    return list(result)


if __name__ == '__main__':
    pass
