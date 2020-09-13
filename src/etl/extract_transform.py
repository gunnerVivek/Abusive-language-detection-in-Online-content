'''
    This module performs Extraction and Transformation of data.
'''

import os
import tweets
import white_supremacy
import wkpd_personal_attacks
import toxic_comments
import fb_hate_speech
from definitions import TRANSFORMED_DATA_DIR


class ExtractTransform:
    '''
        This class contains methods to Extract data from pre-decided sources
        and Transform them according to the project needs.

        Then writes the data sources to a predefined lcation as stated in 
        TRANSFORMED_DATA_DIR.
    '''

    def perform_write(self, data, file_name=None, file_extension='.csv', mode='w'):

        '''Writes the data to disk'''
        
        try:
            # file path (including file name) for write
            _file = os.path.join(TRANSFORMED_DATA_DIR, file_name+file_extension)

            if mode == 'a': # append mode
                if not os.path.isfile(_file):
                    data.to_csv(_file, mode='a', index=False)
                else: # else it exists so append without writing the header
                    data.to_csv(_file, mode='a', header=False, index=False)

            elif mode == 'w': # write mode
                # if: 
                #    file does not exist --> write file
                # else:
                #    overwrite existing file
                data.to_csv(_file, mode='w', index=False)
            else:
                raise ValueError(''' 'mode' argument should be either 'a' (append mode)
                                      or 'w' (write mode). Passed value: {}'''.format(mode)
                                )

        except Exception:
            print('Exception occured while writing {}.'.format(file_name+file_extension))


    def pipeline(self, mode='w'):
        '''
            Fetches data from each of the sources serially.
        '''
        
        ## Twitter data
        self.perform_write(tweets.mine_tweets(), file_name='tweeter_data', mode=mode)
        
        ## White Suupremacy Forum Data
        self.perform_write(white_supremacy.get_white_supremiest_data(), file_name='white_supremist_data', mode=mode)
        
        ## Wikipedia Personal Attacks data
        self.perform_write(wkpd_personal_attacks.get_wikipedia_personal_attacks(), file_name='wikipedia_personal_attacks', mode=mode)
        
        ## Toxic comments
        self.perform_write(toxic_comments.get_toxic_comments(), file_name='toxic_comments', mode=mode)
        
        ## FaceBook Hate Speech
        self.perform_write(fb_hate_speech.get_fb_hate_speech(), file_name='facebook_hate_speech_translated', mode=mode)
        
        ## Unintended Toxic Comments
        self.perform_write(unintended_toxic_comments.get_unintended_toxic_comments(), file_name='unintended_toxic_comments', mode=mode)
        

if __name__ == "__main__":
    ExtractTransform().pipeline()
