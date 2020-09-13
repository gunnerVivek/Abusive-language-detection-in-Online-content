'''
    This module is to extract and Transform data for toxic comments
    from jigsaw-unintended-bias-in-toxicity-classification - converstaional 1
    data source.
'''

from os import path

from pandas import read_csv
from pandas import concat as pd_concat

from definitions import DATA_DIR, TRANSFORMED_DATA_DIR
from etl.definations_configurations import ABUSE, NO_ABUSE


def get_unintended_toxic_comments():

    ## set the path to the data files
    data_path = path.join(DATA_DIR, "jigsaw-unintended-bias-in-toxicity-classification - converstaional 1")
    
    train_data_path = path.join(data_path, 'train.csv')
    test_private_expanded_path = path.join(data_path, 'test_private_expanded.csv')
    test_public_expanded_path = path.join(data_path, 'test_public_expanded.csv')

    ## Read train data
    train = read_csv(train_data_path, usecols=['id', 'comment_text', 'target'])
    train['label'] = train.target.apply(lambda x: ABUSE if x >= 0.5 else NO_ABUSE)


    ## Read test data
    test =  pd_concat([
                    read_csv(test_private_expanded_path, usecols=['id', 'comment_text', 'toxicity']),
                    read_csv(test_public_expanded_path, usecols=['id', 'comment_text', 'toxicity']) 
                    ],
                    ignore_index=True
            )

    # rename column to match train data
    test = test.rename(columns={'toxicity': 'target'})


    ## Combine the two data sets
    data = pd_concat([train, test], ignore_index=True)
    # retain only needed columns
    data['label'] = data.target.apply(lambda x: ABUSE if x >= 0.5 else NO_ABUSE)
    data = data[['comment_text', 'label']]

    return data


if __name__ == "__main__":
    
    write_file = os.path.join(TRANSFORMED_DATA_DIR, 'unintended_toxic_comments.csv')

    data = get_unintended_toxic_comments()
    data.to_csv(write_file, index=False)
