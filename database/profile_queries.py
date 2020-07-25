# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - profile_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from config import DATABASE
from tools.logger import logger

"""
Group of queries to store and retrief data from the profile collections.
The queries start with 'q_' 
Queries return a dict or a lists of dicts when suitable

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
- q_get_a_profile(username)
- q_get_profiles()
- q_save_a_profile(username)
- q_set_profile_scrape_flag(username, flag)

"""
database = DATABASE
collection_name = 'profiles'


def get_collection():
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():  # Todo: add indexes
    pass


def q_get_a_profile(username):
    collection = get_collection()
    q = {'username': username}
    doc = collection.find_one(q)
    return doc


def q_get_profiles():
    collection = get_collection()
    cursor = collection.find().sort([('username', 1)])
    return list(cursor)


def q_save_a_profile(profile):
    collection = get_collection()
    try:
        f = {'user_id': profile['id']}
        u = {'$set': {'username': profile['username'],
                      'name': profile['name'],
                      'bio': profile['bio'],
                      'join_datetime': profile['join_datetime'],
                      'join_date': profile['join_date'],
                      'join_time': profile['join_time'],
                      'url': profile['url'],
                      'location': profile['location'],
                      'private': profile['private'],
                      'verified': profile['verified'],
                      'background_image': profile['background_image'],
                      'avatar': profile['avatar'],
                      },
             '$push': {'timestamp': datetime.now(),
                       'followers': int(profile['followers']),
                       'following': int(profile['following']),
                       'likes': int(profile['likes']),
                       'tweets': int(profile['tweets']),
                       'media': int(profile['media']), }}
        try:
            collection.update_one(f, u, upsert=True)
        except DuplicateKeyError as e:
            raise
    except:
        logger.error(f'Unknown error: {sys.exc_info()[0]}')
        raise


if __name__ == '__main__':
    pass