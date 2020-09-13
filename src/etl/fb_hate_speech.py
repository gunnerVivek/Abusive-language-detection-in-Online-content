import time
import os

from pandas import DataFrame
from pandas import read_csv

import mysql.connector as connector

from google.cloud import translate_v2 as translate

from etl.definations_configurations import ABUSE, NO_ABUSE, FB_DB_CONFIG, GCP_TRANSLATE_CREDENTIALS_PATH


class FBHateSpeech:
    '''
        Get the Hate speech data from Face Book.
        The data is available in a MySQL database.
    '''

    def query_db(self, **config):
        
        '''
            Query the MySQL database.
        '''
        try:
            connection = connector.connect(**config)
            my_cursor = connection.cursor(named_tuple=True, buffered=True)
            
            # It is the main Query
            # Will return duplicate comment Id,
            # thus duplicate messages - due to different annotator ids
            # Make sure to remove duplicate based on all columns
            query = '''
                        SELECT
                            c.message,
                            ac.valence, ac.target_type
                        FROM annotated_comments as ac
                        JOIN comments as c
                        ON ac.comment_id = c.comment_id
                        ; 
                    '''
            my_cursor.execute(query)

            data = my_cursor.fetchall()
            
            data = DataFrame(data)

        except Exception as e:
            print(e) # TODO: Implement Log
        finally:
            if connection.is_connected():
                my_cursor.close()
                connection.close()
                print('MySQL connection closed')

        return data


    def set_gcp_credentials(self):
        
        ''' Set the gcp cerentials '''

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_TRANSLATE_CREDENTIALS_PATH


    def translate_to_english(self, text):

        '''
            translates single text document.

            Uses GCP Translate
        '''

        self.set_gcp_credentials()

        try:
            translate_client = translate.Client()
            result = translate_client.translate(text, target_language='en')['translatedText']
        except Exception as e:
            result = None
        finally:
            return result


    def get_fb_hate_speech(self):
        '''
            main function
            Combines the
        '''
        # query the DB
        data = self.query_db(**FB_DB_CONFIG)
        # remove duplicates returned due to multiple annotators,
        # only if exactly the same
        data = data.drop_duplicates(ignore_index=True)
        
        # Translate to English - calling GCP Translate
        data['translated_message'] = data.message.apply(self.translate_to_english)
        
        # Create albel
        data['label'] = data.target_type.apply(lambda  x: NO_ABUSE if x==1 else ABUSE)
        
        # retain only necessary comments
        data = data[['translated_message', 'label']] # final data

        return data


if __name__ == "__main__":
    write_path = r"processed data/"

    data = FBHateSpeech().get_fb_hate_speech()
    data.to_csv(write_path+'facebook_hate_speech_translated.csv', index=False)
