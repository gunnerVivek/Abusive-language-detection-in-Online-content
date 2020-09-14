'''
	THis module contains all the definations and configurations needed for the etl package.
'''

import pathlib
import os.path

ROOT_DIR = pathlib.Path('root.py').resolve().parent

DATA_DIR = os.path.join(ROOT_DIR, 'data')

TRANSFORMED_DATA_DIR = os.path.join(ROOT_DIR, 'transformed_data')


ABUSE = 'abuse'
NO_ABUSE = 'no_abuse'
