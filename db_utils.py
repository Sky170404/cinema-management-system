import pymysql
from pymongo import MongoClient

DB_CONFIG = {
    'host': 'db',
    'port': 3306,
    'user': 'heidi_user',
    'password': 'heidi_pass',
    'db': 'cinema',
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)

def get_mongo_db():
    client = MongoClient('mongodb://root:root@mongo:27017/')
    return client, client['cinema']