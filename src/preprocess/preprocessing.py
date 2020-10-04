'''
    Engineer features from CLEANED data.
    1. Stop Words
    2. Spelling check - textblob / Spacy
    3. Lemmatize


    Observe the comments from both categories manually
    https://towardsdatascience.com/how-i-improved-my-text-classification-model-with-feature-engineering-98fbe6c13ef3
'''
import spacy
nlp = spacy.load('en_core_web_md')
from spacy.lang.en.stop_words import STOP_WORDS
STOP_WORDS.difference_update(set(['no', 'not', 'dont']))

from textblob import TextBlob

# from pandas import read_csv

# from definitions import CLEANED_DATA_DIR

def stopword_removal(document):
    
    document = str(document)
    document = ' '.join([w for w in document.split() if w not in STOP_WORDS])
    
    return document


def spell_check(document):

    '''
        Uses text blob, because sequnces are not preserved in data.
        Hence, there is no point in using context specific spell check.
    '''
    document = str(TextBlob(document).correct())

    return document


def lemmatize_no_context(document):
    '''
        Perform Lemmatization without considering the context.
        By default spacy tries to consider the context of the documnet,
        but default behavior here is avoided by providing only one token (word)
        at a time as the document, instaed of all the tokens (whole document).
    '''
    document = str(document)

    tokens = document.split()
    lemmas = []
    for token in tokens:
        doc = nlp(token)
        
        # doc will always have one item since,
        # only one word is used as document
        # other wise will need a loop as in:-  for d in doc:
        lemma = doc[0].lemma_
        lemmas.append(lemma)

    return ' '.join(lemmas)


def lemmatize_contextual(document):

    document = str(document)
    doc = nlp(document)
    lemmas = []

    for token in doc:
        lemma = token.lemma_
        lemmas.append(lemma)

    return ' '.join(lemmas)


def prepro_pipe(document, sequence=True):
    '''
        Removed spell checking due to computational limitation
    '''
    document = str(document)
    if not sequence:
        document = stopword_removal(document)
        #document = spell_check(document)
        document = lemmatize_no_context(document)
    else:
        # document = spell_check(document)
        document = lemmatize_contextual(document)

    return document # needs yield


def preprocess(data, text_column='message', sequence=True):
    '''
        Should only be called when run as independent module.
        If used as a part of pipeline, will cause major over head.
    '''
    if sequence:
        data[text_column] = data[text_column].apply(prepro_pipe)
    else:
        data[text_column] = data[text_column].apply(prepro_pipe, args=(sequence))
    return data


if __name__ == "__main__":
    
    preprocess()