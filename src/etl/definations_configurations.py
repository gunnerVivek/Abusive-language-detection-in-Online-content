'''
	THis module contains all the definations and configurations needed for the etl package.
'''

ABUSE = 'abuse'
NO_ABUSE = 'no_abuse'


FB_DB_CONFIG = {
    'user': 'root',
    'password': 'abuse_detection',
    'host': '34.93.153.220',
    'port': 3306,
    'database': 'facebook_hate_speech',
    'raise_on_warnings': True
}


GCP_TRANSLATE_CREDENTIALS_PATH = "E:\Abusive language detection\Deploy SQL-f6f702799144.json"