'''
    Conversational API - 2

    Jigsaw Toxic Comment

    1. All of train.csv

    2. Get the non -1 id from test_label.csv
       get corresponding data from test.csv 
'''

import pandas as pd
from definitions import DATA_DIR
import os


def get_toxic_comments():

    ABUSE = 'abuse'
    NO_ABUSE = 'no_abuse'

    path =  r"jigsaw-toxic-comment-classification-challenge - conversational 2"


    ##########################  test.csv ##############################
    
    test_labels = pd.read_csv(os.path.join(DATA_DIR, path,'test_labels.csv'))

    # filter data sets that is labelled -1
    # this will not have toxicity labels provided
    test_labels = test_labels[test_labels.toxic != -1]

    test_data = pd.read_csv(os.path.join(DATA_DIR, path,'test.csv'))
    # select only those data observations that have labels
    test_data = test_data[test_data['id'].isin(test_labels.id)]

    test_data = pd.merge(test_data, test_labels, how='inner', on='id')
    
    # Label the dataset for overall project consistency
    test_data['label'] = test_data[['toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate']].sum(axis='columns')
    test_data['label'] = test_data.label.apply(lambda x: ABUSE if x > 0 else NO_ABUSE)

    # select only required columns
    # # (63978, 2)
    test_data = test_data[['comment_text', 'label']]

    ##########################  train.csv ##############################
    
    train_data = pd.read_csv(os.path.join(DATA_DIR, path,'train.csv'))
    
    # Label the dataset for overall project consistency
    train_data['label'] = train_data[['toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate']].sum(axis='columns')
    train_data['label'] = train_data.label.apply(lambda x: ABUSE if x > 0 else NO_ABUSE)
    
    # select only required columns
    # (159571, 2)
    train_data = train_data[['comment_text', 'label']]

    # combine train.csv and test.csv
    # (223549, 2)
    toxic_comments = pd.concat([train_data, test_data], ignore_index=True)

    return toxic_comments


if __name__ == "__main__":
    write_path = r"processed data/"
   
    toxic_comments = get_toxic_comments()
    toxic_comments.to_csv(write_path+'toxic_comments.csv', index=False)
