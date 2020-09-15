
import os
import sys

import pathlib

from pandas import read_csv

import mysql.connector as connector

import mysql.connector.errors as mysql_error
import traceback

from definitions import TRANSFORMED_DATA_DIR

from configurations import TRANSFORMED_DATA_DB_CONFIG


# facebook_hate_speech_translated.csv
# 772, translated_message,  label

# toxic_comments.csv
# 2223063, comment_text , label

# tweeter_data.csv
# 67079, tweet, label

# white_supremist_data.csv
# 10703, text, label

# wikipedia_personal_attacks.csv
# 115864, comment, label


class LoadTransformed:
    '''
        Methods to load all of the Transformed data into DB
    '''

    def __setup_db_connection(self, **config):
        '''
            Setup the connection to the DB
        '''
        
        try:
            self.connection = connector.connect(**config)
            self.cursor = self.connection.cursor(buffered=True)
        
        except mysql_error.InterfaceError as e: # data base did not respond (service down or otherwise) | # wrong port | 3 wrong host IP
            print(traceback.format_exc) #Log
            raise # will be caught down the line
            # TODO: Catch the exceptions, log and rethrow to be caught by
        
        except  mysql_error.ProgrammingError as e: # unknown database | # acess denied wrong password | # wrong user
            print(traceback.format_exc) #Log
            raise # will be caught down the line

        except Exception as e: # unknown exception
            print(traceback.format_exc) #Log
            raise # will be caught down the lin

    
    def __close_db_connection(self):
        '''
            close the connection.
        '''
        try:
            if self.connection.is_connected():
                    self.cursor.close()
                    self.connection.close()

        except NameError as e: #happens when connection/cursor is not defined. May be if called after error.
            traceback.format_exc()

        except  Exception as e: # unknown exception
            traceback.format_exc()


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
        except mysql_error.ProgrammingError as e: # TABLE ALREADY EXISTS - for CREATE query
            print(traceback.format_exc())
            raise # pass down the line
        
        # not needed DROP TABLE already checks for IF EXISTS
        except mysql_error.DatabaseError as e: # Unknown table
            print('table `{}` does not exist'.format(table_name))
            print(traceback.format_exc())
            raise # pass down the line

        except Exception as e:
            print(traceback.format_exc()) # Log
            raise # pass down the line



    def __write_to_db(self, dataset, table_name=None):
        '''
            Load data into DB Table
        '''
        # LOAD DATA LOCAL INFILE --> disabled (specifically LOCAL )in MySQL 8
        # by default due to potential security issue.
        try:
            # preps for fast repeated execution
            prepare_stmt = " PREPARE stmt FROM 'INSERT INTO {table_name} (`message`,`label`) VALUES(?,?)'; "
            self.cursor.execute(prepare_stmt)

            insert_stmt  = " EXECUTE stmt USING {message}, {label} "

            for index, row in dataset.iterrows():

                self.cursor.execute(insert_stmt.format(message=row['message'], label=row['label']))
            
            # print('Loading of {table_name} completed'.format(table_name=table_name)) # Log

            deallocate_stmt = "DEALLOCATE PREPARE stmt;"
            self.cursor.execute(deallocate_stmt)

        except mysql_error.DatabaseError as e: # Unknown table - table does not exists
            print('table `{}` does not exist'.format(table_name))
            print(traceback.format_exc())
            raise # pass down the line

        except expression as identifier:
            print(traceback.format_exc())
            raise # pass down the line



    def pipeline(self, data_path_list, files_have_header=True):
        
        '''
            Will be called once for each transformed data source.
            Expects an iterable of data sources.
        '''

        try:
            self.__setup_db_connection(**TRANSFORMED_DATA_DB_CONFIG)
        except Exception:
            print('Failed to Establissh connection with the Database')
            print(traceback.format_exc())
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
            
            except FileNotFoundError as e:
                print('{} not found.'.format(path))
                print(traceback.print_exc()) # Log
                continue # next data source

            except IOError as e:
                # permissin denied and other IO exceptions
                print(traceback.print_exc()) # Log
                continue
            
            except Exception as e:
                # unknown exception
                print(traceback.print_exc()) # Log
                continue
            
            ## 2. Create Table and 3. Insert into table
            try:
                ## 2. create table
                self.__create_table(table_name)
                
                ## 3. write_to_db
                self.__write_to_db(dataset=data, table_name=table_name)

                print('data insert successfull for {}'.format(table_name)) # Log

            except Exception as e:
                # Catching exception thrown from called function
                print(traceback.format_exec())  # Log
                continue
            


        # close the connection
        try:
            self.__close_db_connection()
        except Exception as e:
            print('Failed to close conection properly')


if __name__ == "__main__":
    
    # use pathlib to get all files .csv
    file_pattern = '*.csv'
    file_paths = list(pathlib.Path(TRANSFORMED_DATA_DIR).glob(file_pattern))

    load_transformed = LoadTransformed()

    # call the data sources
    load_transformed.pipeline(file_paths)

    # clean up
    del load_transformed
