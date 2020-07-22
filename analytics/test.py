# --------------------------------------------------------------------------------------------------------
# 2020/07/22
# src - test.py
# md
# --------------------------------------------------------------------------------------------------------

from pymongo import MongoClient

database = 'twitter_database_xxx'


def get_collection(collection_name):  # Todo: same function in many modules. Put in tools?
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


tweets = get_collection('tweets')
profiles = get_collection('profiles')


f={}

u={

}



if __name__ == '__main__':
    pass
