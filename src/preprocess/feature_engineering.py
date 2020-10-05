
'''
    This module works on cleaned and preprocessed data.

    It extracts NLP based features from the text data.

    POS, Dependency Parsing and NER
    for POS count_ is prefixed, because it is postfixed in basic_feature_extracion

    POS TAG List: https://spacy.io/api/annotation
'''
import os.path

from pandas import DataFrame, read_csv

import spacy
nlp = spacy.load('en_core_web_md')
# from spacy.lang.en.stop_words import STOP_WORDS
# STOP_WORDS.difference_update(set(['no', 'not', 'dont']))

from definitions import CLEANED_DATA_DIR
import preprocessing


pos_dict = {'ADJ': 'adjective', 'ADP': 'adposition', 'ADV': 'adverb', 
            'VERB': 'verb', 'INTJ': 'interjection',
            'CONJ':	'conjunction', 'CCONJ': 'conjunction', 'SCONJ': 'conjunction', 
            'NOUN': 'noun', 'PROPN': 'noun', 'PRON': 'pronoun'
}


def get_score_dict():
    score = {'adjective':0, 'adposition':0, 'adverb':0, 'verb':0, 'interjection':0,
            'conjunction':0, 'noun':0,'pronoun':0}
    return score
    

def pos_tagging(document):
    
    document = str(document)

    other = 0
    score_dict = get_score_dict()

    docs = nlp(document)
    for word in docs:
        pos = word.pos_
        if pos_dict.get(pos):
            score_dict[pos_dict.get(pos)]+=1
        else:
            other+=1
    score_dict['other'] = other

    return score_dict


def engineer_features(data):
    '''
        Should only be called when run as independent module.
        If used as a part of pipeline, will cause major over head.
    '''

    df = list(data.message.apply(pos_tagging))
    df = DataFrame(df)
    
    return df



if __name__ == "__main__":


    input_file = os.path.join(CLEANED_DATA_DIR, "train_cleaned.csv")
    output_file = os.path.join(CLEANED_DATA_DIR, "train_post_clean_features.csv")

    data = read_csv(input_file)

    lst = []
    
    for i, row in data.iterrows():
        # print(i, '|', row['message'], '|', row['label'])
        lst.append(pos_tagging(preprocessing.prepro_pipe(row['message'], sequence=False)))
        if (i+1) % 50 == 0:
            logging.info()
            DataFrame(lst).to_csv(output_file, mode='a', index=False)
            lst.clear()

    # any data remaining for the last batch - overflow 
    #data = DataFrame(lst)
    DataFrame(lst).to_csv(output_file, mode='a', index=False)
    # # print(data)
    # data = preprocessing.preprocess(data, sequence=False)
    
    # df_features = engineer_features(data)

    # data = data.merge(df_features,how='inner', left_index=True, right_index=True)

    # print(data)
    # data.to_csv(output_file, mode='a', index=False)
