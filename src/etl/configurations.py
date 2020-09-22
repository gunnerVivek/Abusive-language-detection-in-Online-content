'''
    This module contains all the configurations needed by 
    the modules in the etl package
'''
import os
from definitions import ROOT_DIR

FB_DB_CONFIG = {
    'user': 'root',
    'password': 'abuse_detection',
    'host': '34.93.153.220',
    'port': 3306,
    'database': 'facebook_hate_speech',
    # 'raise_on_warnings': True
}

TRANSFORMED_DATA_DB_CONFIG = {
    'user': 'root',
    'password': 'xxxxxx',
    'host': '35.244.x.xxx',
    'port': 3306,
    'database': 'transformed_data'
    # 'raise_on_warnings': True --> raises exceptions on warnings, ex: for drop if exists, because it causes warnings in MySQL
}

# needed only for translation task
GCP_TRANSLATE_CREDENTIALS_PATH = os.path.join(ROOT_DIR, "src\etl", "xxxxxxxxxx.json")


# Tweeter 
API_KEY = "xxxxxxxxxxxxxx"
API_KEY_SECRET = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

ACCESS_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
ACCESS_TOKEN_SECRET = "xxxxxxxxxxxxxxxxxxxx"
