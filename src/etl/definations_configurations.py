'''
	THis module contains all the definations and configurations needed for the etl package.
'''

ABUSE = 'abuse'
NO_ABUSE = 'no_abuse'


FB_DB_CONFIG = {
    'user': 'root',
    'password': '****',
    'host': '00.00.000.000',
    'port': 3306,
    'database': 'facebook_hate_speech',
    'raise_on_warnings': True
}


GCP_TRANSLATE_CREDENTIALS_PATH = "Path to gcp credentials .json file"


# Tweeter 
API_KEY = "888888888888888888"
API_KEY_SECRET = "888888888888888888888888888888888888"

ACCESS_TOKEN = "99999999999999999999999999999"
ACCESS_TOKEN_SECRET = "999999999999999999999999"