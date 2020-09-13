'''
    This module performs Extraction and Transformation of data.
'''

import os

from tweeter.tweets import Tweets
from white_supremacy import WhiteSupremacy
import wkpd_personal_attacks
from toxic_comments.toxic_comments import ToxicComment
from fb_hate_speech import FBHateSpeech

from definitions import TRANSFORMED_DATA_DIR


class ExtractTransform:
    '''
        This class contains methods to Extract data from pre-decided sources
        and Transform them according to the project needs.

        Then writes the data sources to a predefined location as stated in 
        TRANSFORMED_DATA_DIR.
    '''

    def _perform_write(self, data, file_name, mode='w'):

        '''Writes the data to disk'''
        
        try:
            # file path (including file name) for write
            _file = os.path.join(TRANSFORMED_DATA_DIR, file_name)

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
            print('Exception occured while writing {}.'.format(file_name))


    def pipeline(self, mode='w'):
        '''
            Fetches data from each of the sources serially.
        '''
        
        ## Twitter data
        self._perform_write(Tweets().pipeline(), file_name='tweeter_data', mode=mode)
        
        ## White Suupremacy Forum Data
        self._perform_write(WhiteSupremacy().get_white_supremiest_data(), file_name='white_supremist_data', mode=mode)
        
        ## Wikipedia Personal Attacks data
        self._perform_write(wkpd_personal_attacks.get_wikipedia_personal_attacks(), file_name='wikipedia_personal_attacks', mode=mode)
        
        ## Toxic comments
        self._perform_write(ToxicComment().pipeline(), file_name='toxic_comments', mode=mode)
        
        ## FaceBook Hate Speech
        self._perform_write(FBHateSpeech().get_fb_hate_speech(), file_name='facebook_hate_speech_translated', mode=mode)
        
        ## Unintended Toxic Comments
        self._perform_write(unintended_toxic_comments.get_unintended_toxic_comments(), file_name='unintended_toxic_comments', mode=mode)
        

if __name__ == "__main__":
    ExtractTransform().pipeline()
