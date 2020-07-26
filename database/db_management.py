# --------------------------------------------------------------------------------------------------------
# 2020/07/15
# src - db_management.py
# md
# --------------------------------------------------------------------------------------------------------

"""
A collection of queries to manage the db.

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

IMPLEMENTED QUERIES AND FUNCTIONS
---------------------------------

- q_add_field(collection_name, field_name, value)
- q_remove_field(collection_name, field_name)
- q_rename_field(collection_name, old_field_name, new_field_name)

# - q_copy_field
"""
from datetime import datetime

from pymongo import MongoClient

# from config import DATABASE
from config import DATABASE
from database.twitter_facade import get_a_profile
from tools.logger import logger

database = DATABASE


# Todo: Refactor: put everything in a class. DbManagement.copy_collection().xxx

def get_collection(collection_name):  # Todo: same function in many modules. Put in tools?
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def q_remove_field(collection_name, field_name):
    collection = get_collection(collection_name)
    f = {}
    u = {'$unset': {field_name: 1}}
    result = collection.update_many(f, u, False, None, True)
    logger.info(f'Result remove field {field_name}: {result.raw_result}')


def q_rename_field(collection_name, old_field_name, new_field_name):
    collection = get_collection(collection_name)
    f = {}
    u = {'$rename': {old_field_name: new_field_name}}
    result = collection.update_many(f, u, False, None, True)
    logger.info(f'Result rename field {old_field_name} into {new_field_name}: {result.raw_result}')


def q_add_field(collection_name, field_name, value=None):
    collection = get_collection(collection_name)
    f = {}
    u = {'$set': {field_name: value}}
    result = collection.update_many(f, u, False, None, True)
    logger.info(f'Result adding field {field_name} to value {value}: {result.raw_result}')


# def q_copy_field(collection_name, from_field_name, to_field_name): # Todo:  f'${from_field_name}'}} doesn't work
#     collection = get_collection(collection_name)
#     f = {}
#     u = {'$set': {to_field_name: f'${from_field_name}'}}
#     result = collection.update_many(f, u, False, None, True)
#     logger.info(f'Result copy field {from_field_name} to {to_field_name}: {result.raw_result}')

def blacklist_profiles():
    usernames = [
        'lijstdedecker',  # 0 tweets (2020-07-26)
        'mauritsvdr',  # canceled account
        'elhammouchiothm',  # canceled account
        'ludwigvandenho1',  # 0 tweets (2020-07-26)
    ]
    collection = get_collection('profiles')
    for username in usernames:
        profile = get_a_profile(username)
        if profile:
            f = {'username': profile['username']}
            u = {'$set': {'blacklisted': True,
                          'timestamp': datetime.now()}}
            collection.update_one(f, u)


if __name__ == '__main__':
    pass
