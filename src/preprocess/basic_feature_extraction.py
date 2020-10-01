import spacy
nlp = spacy.load('en_core_web_md')
from spacy.lang.en.stop_words import STOP_WORDS

from pandas import read_csv
import numpy as np

import os.path

from definitions import CLEANED_DATA_DIR


def __load_data(filename):
    '''
        filename: Fully qualified Name of the file
    '''
    data = read_csv(filename, header=0)
    
    return data


def avg_word_len(document):
    '''
        - length of each word and sum them up
        - divide by number of words
    '''
    
    words = str(document).split()
    
    word_len = 0
    for word in words:
        word_len += len(word)
        
    return word_len / len(words)


def is_number(x):
    '''
        Takes a word and checks if Number (Integer or Float)
    '''
    try:
        # only integers and float converts safely
        num = float(x)
        return True
    except ValueError as e: # not convertable to float
        return False

def is_be_verb(word):
    
    word = str(word).lower()
    be_verb = ['am', 'are', 'is', 'was', 'were', 'been', 'being', 'be', 'has', 'and', 'or', 'but', 'have']

    return word in be_verb


def __write_data(data, file_name):
    # 'combined_1.csv'
    data.to_csv(os.path.join(CLEANED_DATA_DIR, file_name), index=False)


def extract_features(data):

    '''
        Pre Cleaning feature extraction.
    '''
    
    # Word count
    data['word_count'] = data['message'].apply(lambda x: len(str(x).split()))
    print('.')

    # Character count
    data['char_count'] = data['message'].apply(lambda x: len( str(x) ))
    print('..')
    
    # Average word length
    data['avg_word_len'] = data['message'].apply(avg_word_len)
    print('...')

    # Upper case word count
    data['upper_case_words'] = data['message'].apply(lambda x: len([word for word in str(x).split() if word.isupper()]))
    print('....')

    # Numeric digit present?
    data['numeric_count'] = data['message'].apply(lambda document: len([word for word in str(document).split() if is_number(word)]))
    print('.....')

    # Word Density - average length of the words
    data['word_density'] = data['char_count'] / (data['word_count']+1)
    print('......')

    # Punctuation count
    punctuations = ['.',',', '"', "'", '-', '?', ':', '!', ';', '<<', '>>', '[', ']', '(', ')' , '{', '}']
    data['punct_count'] = data['message'].apply(lambda x: len([word for word in str(x).split() if word in punctuations]))
    print('......')

    # Stop count
    data['stop_word_count'] = data['message'].apply(lambda x: len([word for word in str(x).split() if word in STOP_WORDS]))
    print('......')

    # AUxillary / be verb/ conjunction count -- Assumption is that it requires a calm and balanced mind to properly use be verbs and hence no abuse
    data['be_verb_count'] = data['message'].apply(lambda x: len([word for word in str(x).split() if is_be_verb(word)]))

    # Unique word count
    data['num_unique_words'] = data['message'].apply(lambda x: len(set(w for w in str(x).split())))
    print('-1')
    
    # Unique words : Number of Words
    data['unique_vs_words'] = data['num_unique_words'] / data['word_count']
    print('0')

    return data


if __name__ == "__main__":
    
    filename = os.path.join(CLEANED_DATA_DIR, 'train.csv')

    __write_data(extract_features(__load_data(filename=filename)), file_name='train_pre_clean_features.csv')
