
import os
import pathlib
from pandas import read_csv

import mysql.connector as connector

from definitions import TRANSFORMED_DATA_DIR
from configurations import GCP_TRANSLATE_CREDENTIALS_PATH


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
        Methods to load all oof the Transformed data into DB
    '''

    @staticmethod
    def __setup_gcp_credentials():
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_TRANSLATE_CREDENTIALS_PATH



    def __create_table(self, table_name, cursor):
        '''
            creates a Single table.

            try Except block not used 
        '''

        drop_query = "DROP TABLE IF EXISTS {table_name};".format(table_name=table_name)

        create_query = '''
                            CREATE TABLE {table_name}(
                                    message TEXT,
                                    label VARCHAR(10)
                            );
                    '''.format(table_name=table_name)

        cursor.execute(drop_query)
        cursor.execute(create_query)


    def __fetch_csv(self, parameter_list):
        pass
    
    def __write_to_db(self, dataset, table_name=None, contains_header=False):
        
        self.__create_table(table_name, cursor)
        
        # LOAD DATA LOCAL INFILE --> disabled (specifically LOCAL )in MySQL 8
        # by default due to potential security issue.

        # preps for fast repeated execution
        prepare_stmt = ''' PREPARE stmt FROM 'INSERT INTO {table_name} (`message`,`label`) VALUES(?,?)'; '''
        cursor.execute(prepare_stmt)

        for index, row in dataset.iterrows():

            insert_stmt  = " EXECUTE stmt USING {message}, {label} ".format(message=row['message'], label=row['label'])
            cursor.execute(insert_stmt)
        
        deallocate_stmt = "DEALLOCATE PREPARE stmt;"
        cursor.execute(deallocate_stmt)

    def pipeline(self, parameter_list):
        
        '''
            Will be called once for each transformed data source.
        '''
        # create table
        # fetch
        # write_to_db

        for data in data_list:
            try:
                # re assign column names for uniformity
                pd.read_csv(data, header=0, names=['message', 'label'])
            except expression as identifier:
                # if access denied exception stop
                # else continue to next data set
                continue




data = read_csv(os.path.join(TRANSFORMED_DATA_DIR, name), squeeze=True, usecols=[0])
print(data.shape)

data = read_csv(os.path.join(TRANSFORMED_DATA_DIR, name), squeeze=True, nrows=5)
print(data)

