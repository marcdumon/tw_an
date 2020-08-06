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
Group of queries to store and retrief data from the profile_stats collections.
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
tweets_collection_name = 'tweets'


def get_collection(col=stat_collection_name):  # Todo: Replace get_collection() with get_collection(col=collection_name) everywhere
    client = MongoClient()
    db = client[database]
    collection = db[col]
    return collection


def setup_stats_collection():
    collection = get_collection()
    collection.create_index([('username', 1), ('year', 1)], unique=True)


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# TWEET STATS
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
        'conversation_id': 1,
        'datetime': 1,
        'tweet': 1,
        'hashtags': 1,
        'reply_to.username': 1,
        'is_reply': 1,
        'nlikes': 1,
        'nreplies': 1,
        'nretweets': 1
    }
    result = collection.find(f, p).sort('datetime')
    return list(result)


def q_upsert_tweet_stat(username, year, month, stats, pos_dict):
    collection = get_collection(stat_collection_name)
    f = {
        'type': 'tweet_stat',
        'username': username,
        'year': year,
        'month': month
    }
    u = {
        '$set':
            {
                'stats': stats,
                'pos': pos_dict,
                'timestamp': datetime.now()
            }
    }
    collection.update_one(f, u, upsert=True)


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# STATS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def q_get_tweet_datetimes(username, begin_date, end_date):
    collection = get_collection('tweets')
    f = {
        'username': username,
        'datetime': {'$gte': begin_date,
                     '$lte': end_date}
    }
    p = {'datetime': 1, '_id': 0}
    result = collection.find(f, p).sort('datetime', ASCENDING)
    return list(result)


def q_bulk_write_profile_stats(requests):
    collection = get_collection(stat_collection_name)
    result = collection.bulk_write(requests)
    logger.debug(f'Bulk result | {result.bulk_api_result}')


def q_upsert_a_profile_stat(username, freq, stat):
    collection = get_collection(stat_collection_name)
    f = {
        'type': 'profile_stat',
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


def q_update_a_profile_stat(username, freq, stat):
    collection = get_collection(stat_collection_name)
    f = {
        'type': 'profile_stat',
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


def q_update_profile_stats(username, freq, profile_stats):
    # Todo: This method is a bit hacky and could probably be improved.
    #       The method receives a list of profile_stats-dicts for  a certain freq. It then tries to update
    #       the stat in the array f'{freq}'. It checks it the datetime matches. If true then it uses the position operator $
    #       to find the index of datetime in f'{freq}'.datetime, and uses that index to update the stat.
    #       If the new stat has a different value then it gets updated and result.modified_count is not 0.
    #       If the new stat is exactly the same as the stored one, the it doesn't get updated. The timestamp field ensures that even in
    #       that case, the stat will be updated, otherwise the same stat will be pushed.
    #       If the stat does'n texist, result.modified_count is 0 and the stat will be pushed. We can't use upsert here because positionqal operator doesn't work with
    #       upsert.
    # Inspiration: http://blog.rcard.in/database/mongodb/time-series/2017/01/31/implementing-time-series-in-mongodb.html
    collection = get_collection(stat_collection_name)
    for stat in profile_stats.to_dict('records'):
        result = q_update_a_profile_stat(username, freq, stat)
        if not result.modified_count:  # No update means that the document or stat doesn't exists
            try:
                q_upsert_a_profile_stat(username, freq, stat)
            except DuplicateKeyError:  # For when inserting iso upserting.
                logger.debug(f'DuplicateKeyError | {username}, {freq}, {stat}')
            except:
                print(sys.exc_info())
                raise


def q_populate_profile_stats(username, freq, profile_stats):
    """
    todo: This only works with a unique index on username+year
    """
    logger.warning('This only works with a unique index on username+year!')
    collection = get_collection(stat_collection_name)
    dts = profile_stats['datetime'].to_list()
    years = sorted(set([dt.year for dt in dts]))
    for year in years:
        year_stat = profile_stats.loc[datetime(year, 1, 1, 0, 0, 0):datetime(year, 12, 31, 23, 59, 59)]

        # Try to insert new document with empty profile_stats. Make sure there is a unique index on username+year !
        d = {
            'type': 'profile_stat',
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
        # Now are sure that the empty doc exist. Lets bulk update it with profile_stats
        requests = []
        for stat in year_stat.to_dict('records'):
            f = {
                'type': 'profile_stat',
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
        q_bulk_write_profile_stats(requests)


if __name__ == '__main__':
    pass
    # q_delete_all_stats()
