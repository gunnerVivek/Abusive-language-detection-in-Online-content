'''
    This module performs cleaning steps on each individual
    dataset, that are particular to the datsets only. The 
    only generic step is de-duplication of the data.

    However for toxic_comments dataset no peculiarities were 
    observed. All the required cleaning steps were of generic in nature.

    After cleaning and de-duplication of the individual datasets,
    all of the datasets are combined and stored locally to avoid
    any further Network I/O.

    Expectation is next step is General Feature Extraction (Pre Cleaning).

    Exploration details that helped in identifying these steps
    could be found in the notebook mentioned below.

    location:- NoteBooks\pecularities_data_source.ipynb
'''


import os.path
import re

from pandas import Series, DataFrame, read_csv

import mysql.connector as connector
import mysql.connector.errors as mysql_error

import logging
import logging.config

from configurations_2 import TRANSFORMED_DATA_DB_CONFIG
from definitions import TRANSFORMED_DATA_DIR, CLEANED_DATA_DIR


# get log configuration from file
logging.config.fileConfig("fl_name.conf")
    

class SpecificClean:

    def __init__(self):
        # declare the loggers
        pass
    

    @classmethod
    def __connect(cls, **config):

        cls.connection = connector.connect(autocommit=False, **config) # CHANGED
        cls.cursor = cls.connection.cursor(prepared=True) #buffered=True, 


    def __setup_db_connection(self, **config):
        '''
            Setup the connection to the DB
        '''
        try:
            SpecificClean.__connect(**TRANSFORMED_DATA_DB_CONFIG)
            self.load_logger.info('Connected to DB.')
        
        except mysql_error.InterfaceError as e: 
            # data base did not respond (service down or otherwise)
            #  | # wrong port | 3 wrong host IP
            
            self.err_logger.critical("Database did not respond")
            raise # will be caught down the line
            # TODO: Catch the exceptions, log and rethrow to be caught by
        
        except  mysql_error.ProgrammingError as e: 
            # unknown database | # acess denied wrong password | # wrong user
            # print(traceback.format_exc) #Log
            self.err_logger.critical("Database credentials Faulty.")
            raise # will be caught down the line

        except Exception as e: # unknown exception
            # print(traceback.format_exc) #Log
            self.err_logger.critical(e, exc_info=True)
            raise # will be caught down the lin    
    

    def __close_db_connection(self):
        '''
            close the connection.
        '''
        try:
            if SpecificClean.connection.is_connected():
                    SpecificClean.cursor.close()
                    SpecificClean.connection.close()
            
            self.load_logger.info("Connection to Database: {db_name} closed"\
                                  .format(db_name=TRANSFORMED_DATA_DB_CONFIG['database'])
                                )

        except NameError as e: #happens when connection/cursor is not defined. May be if called after error.
            self.err_logger.exception('Erronemous call to close conection method. Connection Object does not exist')
            # traceback.format_exc()

        except  Exception as e: # unknown exception
            # traceback.format_exc()
            sel.err_logger.error(e, exc_info=True)

    # def __get_table_names(self):
                
        # # query = "SELECT * FROM wikipedia_personal_attacks LIMIT 100;"
        # query = "SHOW TABLES;"
        # SpecificClean.cursor.execute(query)

        # result = SpecificClean.cursor.fetchall()
        # result = [x[0] for x in result]

        # return result

    
    def __read_data(self, table_name):
        
        # TODO: use connection to db

        query = f"SELECT * FROM {table_name};"

        SpecificClean.cursor.execute(query)

        result = SpecificClean.cursor.fetchall()


        # result = read_csv(os.path.join(TRANSFORMED_DATA_DIR, table_name+".csv"),
        #                  names=['message', 'label'], header=0)
        return result


    def __facebook_hate_speech_translated(self):
        
        data = self.__read_data("facebook_hate_speech_translated")
        # data = DataFrame(data, columns=['message', 'label']) # TODO
        
        data = data.drop_duplicates(subset=['message'])

        remove_quot_decimal = lambda x: re.sub("&quot;", '', x)
        remove_apostrophe_decimal = lambda x: re.sub("&#39;", "'", x)
        remove_trailing_leading_spaces = lambda x: x.strip()

        data['message'] = data['message'].apply(remove_quot_decimal)\
                                         .apply(remove_apostrophe_decimal)\
                                         .apply(remove_trailing_leading_spaces)

        return data


    # def __toxic_comments(self):
        
    #     data = self.__read_data("toxic_comments")
    #     # data = DataFrame(data, columns=['message', 'label']) # TODO
        
    #     data = data.drop_duplicates(subset=['message'])

    #      = lambda x: re.sub("\n", "", x)
    #      = lambda x: re.sub("\r", "", x)
    #     remove_trailing_leading_spaces = lambda x: x.strip()

    #     data['message'] = data['message'].apply(remove_quot_decimal)\
    #                                      .apply(remove_apostrophe_decimal)\
    #                                      .apply(remove_trailing_leading_spaces)

        
    #     return data


    def __tweeter_data(self):
        data = self.__read_data("tweeter_data")
        # data = DataFrame(data, columns=['message', 'label']) # TODO
        
        data = data.drop_duplicates(subset=['message'])

        remove_RT = lambda x: str(x).replace("RT", "")
        remove_trailing_leading_spaces = lambda x: str(x).strip()

        data['message'] = data['message'].apply(remove_RT)\
                                         .apply(remove_trailing_leading_spaces)

        return data


    def __white_supremist_data(self):
        data = self.__read_data("white_supremist_data")
        # data = DataFrame(data, columns=['message', 'label']) # TODO
        
        data = data.drop_duplicates(subset=['message'])

        remove_dqoute_squarebracket = lambda x: str(x).replace('["', '').replace('"]', '')
        remove_sqoute_squarebracket = lambda x: str(x).replace("['", '').replace("']", '')
        remove_trailing_leading_spaces = lambda x: str(x).strip()

        data['message'] = data['message'].apply(remove_dqoute_squarebracket)\
                                         .apply(remove_sqoute_squarebracket)\
                                         .apply(remove_trailing_leading_spaces)

        return data


    def __wikipedia_personal_attacks(self):
        data = self.__read_data("wikipedia_personal_attacks")
        # data = DataFrame(data, columns=['message', 'label']) # TODO
        
        data = data.drop_duplicates(subset=['message'])

        remove_NEWLINE_TOKEN = lambda x: str(x).replace("NEWLINE_TOKEN", "")
        remove_doubleticks = lambda x: str(x).replace("``", "")
        remove_UTC = lambda x: str(x).replace("(UTC)", "")
        remove_trailing_leading_spaces = lambda x: str(x).strip()

        data['message'] = data['message'].apply(remove_NEWLINE_TOKEN)\
                                         .apply(remove_doubleticks)\
                                         .apply(remove_UTC)\
                                         .apply(remove_trailing_leading_spaces)

        return data


    def __write_to_disk(self, data=None, write_file_name=None):
        
        if not isinstance(data, DataFrame):
            data = DataFrame(data, columns=['message', 'label'])
        
        # append to existing file
        # if file does not exist create with header
        data.to_csv(write_file_name, mode='a', header= not os.path.isfile(write_file_name), index=False)


    def pipeline(self, file_name='combined.csv'):

        self.__setup_db_connection(**TRANSFORMED_DATA_DB_CONFIG) # TODO

        # combined data file full name
        write_file_name = os.path.join(CLEANED_DATA_DIR, file_name)

        self.__write_to_disk(data=self.__facebook_hate_speech_translated(),
                             write_file_name=write_file_name)

        # write only, no cleaning
        self.__write_to_disk(data=self.__read_data("toxic_comments"),
                             write_file_name=write_file_name)

        self.__write_to_disk(data=self.__tweeter_data(),
                             write_file_name=write_file_name)

        self.__write_to_disk(data=self.__white_supremist_data(),
                             write_file_name=write_file_name)

        self.__write_to_disk(data=self.__wikipedia_personal_attacks(),
                             write_file_name=write_file_name)
        


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


if __name__ == "__main__":
    SpecificClean().pipeline("combined.csv")
    