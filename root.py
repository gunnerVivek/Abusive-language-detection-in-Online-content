'''
    This is adummy module.
    It works an a marker for root directory.
'''


import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(ROOT_DIR, 'data')

TRANSFORMED_DATA_DIR = os.path.join(ROOT_DIR, 'transformed_data')
