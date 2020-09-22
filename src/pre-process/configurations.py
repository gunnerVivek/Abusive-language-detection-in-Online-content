'''
    This module contains all the configurations needed by 
    the modules in the etl package
'''
# import os
# from definitions import ROOT_DIR


TRANSFORMED_DATA_DB_CONFIG = {
    'user': 'root',
    'password': 'xxxxxxxxxxxx',
    'host': '35.244.x.xxx',
    'port': 3306,
    'database': 'transformed_data'
    # 'raise_on_warnings': True --> raises exceptions on warnings, ex: for drop if exists, because it causes warnings in MySQL
}

