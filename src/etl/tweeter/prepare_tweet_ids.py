'''
    This module gets the unique tweet Ids
    for all of the Tweeter based data sets

    data source - 11 and 24
'''

import os.path
import collections
import pathlib

from definitions import DATA_DIR
from  etl.definations_configurations import ABUSE, NO_ABUSE

row  = collections.namedtuple('row', ['tweet_id', 'label'])

####################### 11 - Hateful Symbols or Hateful People #####################

def _get_source_1_tweet_ids():

    file_name = os.path.join(DATA_DIR, "11 - Hateful Symbols or Hateful People", "NAACL_SRW_2016.csv")
    
    tweet_ids = []

    with open(file_name, 'r') as file:
        # tweet_ids = [line.split(',')[0] for line in file.read().splitlines()]
        for line in file.read().splitlines():
            
            tweet_ids.append(row(tweet_id=line.split(',')[0],
                                 label= ''.join([NO_ABUSE
                                                  if line.split(',')[1].lower() == 'none'
                                                   else ABUSE]
                                                )
                                )
                            ) 

    return tweet_ids


############################# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ##########################


############## 24 - Peer to Peer Hate: Hate Speech Instigators and Their Targets  ###########

def _get_source_2_tweet_ids():

    '''
        This source files only has tweet ids of abusive tweets.
    '''

    # peer_to_peer_hate = r"E:\Abusive language detection\data\24 - hate_speech_icwsm18-master"
    peer_to_peer_hate = r"data\24 - hate_speech_icwsm18-master"


    hash_tag = r"\twitter_hashtag_based_datasets"
    key_phrase = r"\twitter_key_phrase_based_datasets"

    file_pattern = '*.csv'


    tweet_ids = []

    for sub_dir in [hash_tag, key_phrase]: #traverse each subdirectory

        # get file names in that sub-directory
        file_names = list(pathlib.Path(peer_to_peer_hate+sub_dir).glob(file_pattern))

        # Traverse each file
        for name in file_names:

            # read the tweet Ids in each file
            with open(name, mode='r') as csvfile:
                # append Id to the list
                for Id in csvfile.read().splitlines(): # splitlines gets rid of '\n' character
                    tweet_ids.append(row(tweet_id=Id, label= ABUSE))

    return tweet_ids

############################# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ##########################


def get_tweet_ids_labels():

    '''
        Collect all tweet ids and labels.
    '''

    tweet_ids = []

    tweet_ids.extend(_get_source_1_tweet_ids())
    tweet_ids.extend(_get_source_2_tweet_ids())

    # 'row'--> ['tweet_id', 'label']
    return tweet_ids


if __name__ == "__main__":
    
    # should not be called in Standalone
    pass
