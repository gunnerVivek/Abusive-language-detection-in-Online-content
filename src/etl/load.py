
import os
import sys

import pathlib

from pandas import read_csv

import mysql.connector as connector

import mysql.connector.errors as mysql_error
import traceback

from definitions import TRANSFORMED_DATA_DIR

from configurations import TRANSFORMED_DATA_DB_CONFIG

import logging
import logging.config


# get log configuration from file
logging.config.fileConfig("etl_logging.conf")


class LoadTransformed:
    '''
        Methods to load all of the Transformed data into DB
    '''

    def __init__(self):
        # declare the loggers 
        self.err_logger = logging.getLogger("errLogger")
        self.load_logger = logging.getLogger("root")
        
        self.load_logger.info("######## NEW EXECUTION ###############")
        self.load_logger.info('LoadTransformed object initialised.')


    def __setup_db_connection(self, **config):
        '''
            Setup the connection to the DB
        '''
        
        try:
            self.connection = connector.connect(autocommit=False, **config) # CHANGED
            self.cursor = self.connection.cursor(prepared=True) #buffered=True, 

            self.load_logger.info('Connected to DB.')
        
        except mysql_error.InterfaceError as e: 
            # data base did not respond (service down or otherwise)
            #  | # wrong port | 3 wrong host IP
            # print(traceback.format_exc) #Log
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
            if self.connection.is_connected():
                    self.cursor.close()
                    self.connection.close()
            
            self.load_logger.info("Connection to Database: {db_name} closed"\
                                  .format(db_name=TRANSFORMED_DATA_DB_CONFIG['database'])
                                )

        except NameError as e: #happens when connection/cursor is not defined. May be if called after error.
            self.err_logger.exception('Erronemous call to close conection method. Connection Object does not exist')
            # traceback.format_exc()

        except  Exception as e: # unknown exception
            # traceback.format_exc()
            sel.err_logger.error(e, exc_info=True)


    def __create_table(self, table_name):
        '''
            creates a Single table.

            if exception occured, will return the exception
        '''

        try:
            drop_query = "DROP TABLE IF EXISTS {table_name};".format(table_name=table_name)

            create_query = '''
                                CREATE TABLE {table_name}(
                                        message TEXT,
                                        label VARCHAR(10)
                                );
                        '''.format(table_name=table_name)

            self.cursor.execute(drop_query)
            self.cursor.execute(create_query)

            self.load_logger.info("Executed create table query for Table: {0}".format(table_name))

        except mysql_error.ProgrammingError as e: # TABLE ALREADY EXISTS - for CREATE query
            # print(traceback.format_exc())
            self.err_logger.error(e, exc_info=True)
            raise # pass down the line
        
        # not needed DROP TABLE already checks for IF EXISTS
        except mysql_error.DatabaseError as e: # Unknown table
            # print(traceback.format_exc())
            self.err_logger.error(e, exc_info=True)
            raise # pass down the line

        except Exception as e:
            self.err_logger.error(e, exc_info=True)
            raise # pass down the line



    def __write_to_db(self, dataset, table_name=None):
        '''
            Load data into DB Table
            dataset is a data collection of multiple rows.
        '''
        # LOAD DATA LOCAL INFILE --> disabled (specifically LOCAL )in MySQL 8
        # by default due to potential security issue.
        try:

            _insert_query = '''
                                INSERT INTO {table_name} (`message`,`label`) 
                                VALUES(%s,%s)
                           '''.format(table_name=table_name)
            # insert_stmt  = " EXECUTE stmt USING {message}, {label};"

            # this will hold rows of data for each transaction
            transaction_data_list = []

            for index, row in dataset.iterrows():

                transaction_data_list.append((row['message'], row['label']))

                # for batch size == 500
                # Insert data into DB and commit 
                if index>0 and index % 500 == 0:
                    self.connection.start_transaction()

                    self.cursor.executemany(_insert_query, transaction_data_list)

                    self.connection.commit()

                    # empty the transaction data list
                    transaction_data_list.clear()

                    self.load_logger.info(f"{index} rows commited in {TRANSFORMED_DATA_DB_CONFIG['database']}.{table_name}")
               

            # this is to make sure all data at last is commited
            # any data rows not commited after last buffer size block is crossed
            # will also be commited
            self.connection.commit() 

            self.load_logger.info(f'Loading of {table_name} completed')

        except mysql_error.DatabaseError as e: # Unknown table - table does not exists
            self.connection.rollback()
            self.err_logger.exception(f'table `{table_name}` does not exist')
            # print(traceback.format_exc())
            raise # pass down the line

        except Exception as e:
            self.connection.rollback()
            self.err_logger.error(e, exc_info=True)
            raise # pass down the line



    def pipeline(self, data_path_list, files_have_header=True):
        
        '''
            Will be called once for each transformed data source.
            Expects an iterable of data sources.
        '''

        try:
            self.__setup_db_connection(**TRANSFORMED_DATA_DB_CONFIG)

            self.load_logger.info('Connection set up method called from pipeline()')

        except Exception as e:
            self.load_logger.info('Failed to establish connection to DB, from pipeline()')
            self.err_logger.exception('Exiting with exit code = 1')
            # print(traceback.format_exc())
            sys.exit(1)

        # iterate through each source
        for path in data_path_list:

            # c:/repositories/abc.csv --> abc
            table_name = pathlib.Path(path).stem
            
            ## 1. fetch data from file
            try:
                if files_have_header:

                    # first row is expected to be header and will be skipped
                    # re assign column names for uniformity
                    data = read_csv(path, header=0, names=['message', 'label'])

                else:
                    # assign column names
                    data = read_csv(path, names=['message', 'label'])

                self.load_logger.info(f"{pathlib.Path(path).name} read from file")

            except FileNotFoundError as e:
                # print('{} not found.'.format(path))
                # print(traceback.print_exc()) # Log
                self.err_logger.exception(f'File NOt found: {path}')
                continue # next data source

            except IOError as e:
                # permissin denied and other IO exceptions
                # print(traceback.print_exc()) # Log
                self.err_logger.exception("permissin denied or other IO exceptions")
                continue
            
            except Exception as e:
                # unknown exception
                # print(traceback.print_exc()) # Log
                self.err_logger.error(e, exc_info=True)
                continue
            
            ## 2. Create Table and 3. Insert into table
            try:
                ## 2. create table
                self.__create_table(table_name)
                
                self.load_logger.info(f"Create table called from pipeline() for file: {pathlib.Path(path).name}")

                ## 3. write_to_db
                self.__write_to_db(dataset=data, table_name=table_name)

                self.load_logger.info(f"write into DB called from pipeline() for file: {pathlib.Path(path).name}")
                # print('data insert successfull for {}'.format(table_name)) # Log

            except Exception as e:
                # Catching exception thrown from called function
                # print(traceback.format_exc())  # Log
                self.err_logger.error(e, exc_info=True)
                continue
            

        # close the connection
        try:
            self.__close_db_connection()
        except Exception as e:
            # print('Failed to close conection properly')
            self.err_logger.exception("Failed to close conection properly")


if __name__ == "__main__":
    
    # use pathlib to get all files .csv
    # file_pattern = '*.csv' # CHANGED
    # file_paths = list(pathlib.Path(TRANSFORMED_DATA_DIR).glob(file_pattern)) # CHANGED


    # E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\facebook_hate_speech_translated.csv
    # E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\toxic_comments.csv
    # E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\tweeter_data.csv
    # E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\white_supremist_data.csv
    # E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\wikipedia_personal_attacks.csv
    
    load_transformed = LoadTransformed()
    file_paths = [
                  r"E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\wikipedia_personal_attacks.csv"
    ] # r"E:\repositories\Abusive-language-detection-in-Online-content\transformed_data\toxic_comments.csv",
    
    # CHANGED
    # call the data sources

    load_transformed.pipeline(file_paths)

    # clean up
    del load_transformed
