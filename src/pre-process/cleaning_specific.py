'''
    This module performs cleaning steps on each individual
    dataset, that are particular to the datsets only. The 
    only generic step is de-duplication of the data.

    After cleaning and de-duplication of the individual datasets,
    all of the datasets are combined and stored locally to avoid
    any further Network I/O.

    Expectation is next step is General Feature Extraction (Pre Cleaning).

    Exploration details that helped in identifying these steps
    could be found in the notebook mentioned below.

    location:- NoteBooks\pecularities_data_source.ipynb
'''

from configurations_2 import TRANSFORMED_DATA_DB_CONFIG

import mysql.connector as connector
import mysql.connector.errors as mysql_error
import re

from pandas import Series

# import logging
# import logging.config


# get log configuration from file
# logging.config.fileConfig("etl_logging.conf")

class Switcher(object):
      
    def switch(self,method_name):
        method=getattr(self,method_name,lambda :'Invalid')
        return method()


    def facebook_hate_speech_translated(self, column):
        if not isinstance(column, Series):
            column = Series(column)

        column = column
        re.sub()
        return 'fb'

    def toxic_comments(self):
        return 'wiki'
    def tweeter_data(self):
        return 'hate'
    def white_supremist_data()
        pass
    def wikipedia_personal_attacks():
        pass


class SpecificClean:

    def __init__(self):
        # declare the loggers
        pass
    
    # declaring class 
    # connection = None
    # cursor = None

    @classmethod
    def __connect(cls, **config):

        cls.connection = connector.connect(autocommit=False, **config) # CHANGED
        cls.cursor = cls.connection.cursor(prepared=True) #buffered=True, 


    def __setup_db_connection(self, **config):
        '''
            Setup the connection to the DB
        '''
        
        SpecificClean.__connect(**TRANSFORMED_DATA_DB_CONFIG)
        
        # self.load_logger.info('Connected to DB.')


    def __get_table_names(self):
                
        # query = "SELECT * FROM wikipedia_personal_attacks LIMIT 100;"
        query = "SHOW TABLES;"
        SpecificClean.cursor.execute(query)

        result = SpecificClean.cursor.fetchall()
        result = [x[0] for x in result]

        return result

    def __cleaning_steps():
        pass

    def __read_data(self, parameter_list):
        pass

    def pipeline(self):

        self.__setup_db_connection(**TRANSFORMED_DATA_DB_CONFIG)
        table_names = self.__get_table_names()

    # def __get_table_names(self):


    # def __setup_db_connection(self, **config):
    #     '''
    #         Setup the connection to the DB
    #     '''
        
    #     try:
    #         # connection = connector.connect(autocommit=False, **config) # CHANGED
    #         # cursor = connection.cursor(prepared=True) #buffered=True, 
    #         SpecificClean.__connect(**TRANSFORMED_DATA_DB_CONFIG)
    #         # self.load_logger.info('Connected to DB.')
        
    #     except mysql_error.InterfaceError as e: 
    #         # data base did not respond (service down or otherwise)
    #         #  | # wrong port | 3 wrong host IP
            
    #         self.err_logger.critical("Database did not respond")
    #         raise # will be caught down the line
    #         # TODO: Catch the exceptions, log and rethrow to be caught by
        
    #     except  mysql_error.ProgrammingError as e: 
    #         # unknown database | # acess denied wrong password | # wrong user
    #         # print(traceback.format_exc) #Log
    #         self.err_logger.critical("Database credentials Faulty.")
    #         raise # will be caught down the line

    #     except Exception as e: # unknown exception
    #         # print(traceback.format_exc) #Log
    #         self.err_logger.critical(e, exc_info=True)
    #         raise # will be caught down the lin


    # def funcname(self, parameter_list):
    #     pass

if __name__ == "__main__":
    SpecificClean().pipeline()
    
# SpecificClean()._get_table_names()