# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - tweet_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from config import DATABASE
from tools.logger import logger

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
- q_get_nr_tweets_per_day(username, session_begin_date, session_end_date)
- q_save_a_tweet(tweet)
- q_update_a_tweet(tweet)
"""

database = DATABASE
collection_name = 'tweets'


def get_collection():
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():  # Todo: add indexes
    collection = get_collection()
    collection.create_index('tweet_id', unique=True)




def q_save_a_tweet(tweet):
    collection = get_collection()
    try:
        result = collection.insert_one(tweet)
    except DuplicateKeyError as e:
        logger.debug(f"Duplicate: {tweet['tweet_id']} - {tweet['date']} - {tweet['name']}")
    except:
        logger.error(f'Unknown error: {sys.exc_info()[0]}')
        raise


def q_update_a_tweet(tweet):  # Todo: replace tweet with its fields
    collection = get_collection()
    f = {'tweet_id': tweet['tweet_id']}
    tweet.update({'timestamp': datetime.now()})
    u = {'$set': tweet}
    try:
        result = collection.update_one(f, u, upsert=True)
        logger.debug(f"Updated: {result.raw_result} | {tweet['date']} {tweet['name']}")
    except DuplicateKeyError as e:
        logger.error(f"Duplicate: {tweet['tweet_id']} - {tweet['date']} - {tweet['name']}")



if __name__ == '__main__':
    pass
