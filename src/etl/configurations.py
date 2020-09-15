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
    'raise_on_warnings': True
}

TRANSFORMED_DATA_DB_CONFIG = {
    'user': 'root',
    'password': 'abuse_detection',
    'host': '35.244.1.220',
    'port': 3306,
    'database': 'transformed_data',
    'raise_on_warnings': True
}

# needed only for translation task
GCP_TRANSLATE_CREDENTIALS_PATH = os.path.join(ROOT_DIR, "src\etl", "Deploy SQL-f6f702799144.json")


# Tweeter 
API_KEY = "ZXbDc8BX194cWy79DmKIspDfb"
API_KEY_SECRET = "CZAe4TVFlUmNPgABBLOBEWgvI4d9THA5IIhxxghcJwGW4clkC7"

ACCESS_TOKEN = "111951374-TNuHwvgSs4Fg3eeFaoXLheQMfNiAlt2ODZqHmRS7"
ACCESS_TOKEN_SECRET = "0IjrCabolJocTFLqvioS2nUAIqrHL5SiYs58Dy3INreHT"
