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