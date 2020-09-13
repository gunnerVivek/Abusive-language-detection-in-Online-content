'''
    data set 9
'''

# import csv
import os.path
from collections import namedtuple

from definitions import DATA_DIR, TRANSFORMED_DATA_DIR
from etl.definations_configurations import ABUSE, NO_ABUSE

def get_pre_downloaded_tweets_labels():

    row = namedtuple('row', ['tweet','label'])
    tweets = []

    path = os.path.join(DATA_DIR, "9 - Automate Hate speech detection\labelled_data.csv")

    with open(path, 'r') as csv_file:
        
        for i, line in enumerate(csv_file.read().splitlines()):

            if i == 0: # first line is the header row
                
                tweets.append(row(tweet='tweet', label='label'))
            else:
                # since Id is not returned; hence only append if not empty
                # check for empty/None and all fields are present in the field
                if line and len(line.split(",")) == 7: #and line != '' : 
                    # ['', 'count', 'hate_speech', 'offensive_language', 'neither', 'class', 'tweet']
                    tokens = line.split(",")
                    
                    if tokens[2] or tokens[3]:
                        label = ABUSE
                    else:
                        label = NO_ABUSE

                    tweets.append(row(tweet=tokens[6], label=label))

    return tweets


if __name__ == "__main__":
   
   write_file = os.path.join(TRANSFORMED_DATA_DIR, 'pre_downloaded_tweets.csv')
    
   data = get_pre_downloaded_tweets_labels()
   data.to_csv(write_file, index=False)
