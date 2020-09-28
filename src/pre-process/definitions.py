'''
	THis module contains all the definations and configurations needed for the etl package.
'''

import pathlib
import os.path

ROOT_DIR = pathlib.Path('root.py').resolve().parent

DATA_DIR = os.path.join(ROOT_DIR, 'data')

TRANSFORMED_DATA_DIR = os.path.join(ROOT_DIR, 'transformed_data')

CLEANED_DATA_DIR = os.path.join(ROOT_DIR, 'cleaned_data')

ABUSE = 'abuse'
NO_ABUSE = 'no_abuse'


contractions = {
     "'aight": 'alright',
     "ain't": "not",
     "amn't": 'am not',
     "aren't": 'are not',
     "can't": 'cannot',
     "'cause": 'because',
     "could've": 'could have',
     "couldn't": 'could not',
     "couldn't've": 'could not have',
     "daren't": 'dare not',
     "daresn't": 'dare not',
     "dasn't": 'dare not',
     "didn't": 'did not',
     "doesn't": 'does not',
     "don't": 'do not',
     'dunno': "do not know",
     "d'ye": 'do you',
     "e'er": 'ever',
     "everybody's": 'everybody is',
     "everyone's": 'everyone is',
     'finna': 'fixing to',
     "g'day": 'good day',
     'gimme': 'give me',
     "giv'n": 'given',
     'gonna': 'going to',
     'gotta': 'got to',
     "hadn't": 'had not',
     "had've": 'had have',
     "hasn't": 'has not',
     "haven't": 'have not',
     "he'd": 'he had',
     "he'll": 'he will',
     "he's": 'he is',
     "he've": 'he have',
     "how'd": 'how did',
     'howdy': 'how do you do',
     "how'll": 'how will',
     "how're": 'how are',
     "how's": 'how is',
     "I'd": 'I had',
     "I'd've": 'I would have',
     "I'll": 'I will',
     "I'm": 'I am',
     "I'm'a": 'I am about to',
     "I'm'o": 'I am going to',
     'innit': 'is it not',
     "I've": 'I have',
     "isn't": 'is not',
     "it'd": 'it would',
     "it'll": 'it will',
     "it's": 'it is',
     'iunno': "I don't know",
     "let's": 'let us',
     "ma'am": 'madam',
     "mayn't": 'may not',
     "may've": 'may have',
     'methinks': 'me thinks',
     "mightn't": 'might not',
     "might've": 'might have',
     "mustn't": 'must not',
     "mustn't've": 'must not have',
     "must've": 'must have',
     "needn't": 'need not',
     'nal': 'and all',
     "ne'er": 'never',
     "o'clock": 'of the clock',
     "o'er": 'over',
     "ol'": 'old',
     "oughtn't": 'ought not',
     "'s": ' is',
     "shalln't": 'shall not',
     "shan't": 'shall',
     "she'd": 'she had',
     "she'll": 'she will',
     "she's": 'she is',
     "should've": 'should have',
     "shouldn't": 'should not',
     "shouldn't've": 'should not have',
     "somebody's": 'somebody is',
     "someone's": 'someone is',
     "something's": 'something is',
     "so're": 'so are',
     "that'll": 'that will',
     "that're": 'that are',
     "that's": 'that is',
     "that'd": 'that would',
     "there'd": 'there would',
     "there'll": 'there will',
     "there're": 'there are',
     "there's": 'there is',
     "these're": 'these are',
     "these've": 'these have',
     "they'd": 'they had',
     "they'll": 'they will',
     "they're": 'they are',
     "they've": 'they have',
     "this's": 'this is',
     "those're": 'those are',
     "those've": 'those have',
     "'tis": 'it is',
     "to've": 'to have',
     "'twas": 'it was',
     'wanna': 'want to',
     "wasn't": 'was not',
     "we'd": 'we would',
     "we'd've": 'we would have',
     "we'll": 'we will',
     "we're": 'we are',
     "we've": 'we have',
     "weren't": 'were not',
     "what'd": 'what did',
     "what'll": 'what will',
     "what're": 'what are',
     "what's": 'what is',
     "what've": 'what have',
     "when's": 'when is',
     "where'd": 'where did',
     "where'll": 'where will',
     "where're": 'where are',
     "where's": 'where is',
     "where've": 'where have',
     "which'd": 'which had',
     "which'll": 'which will',
     "which're": 'which are',
     "which's": 'which is',
     "which've": 'which have',
     "who'd": 'who would',
     "who'd've": 'who would have',
     "who'll": 'who will',
     "who're": 'who are',
     "who's": 'who is',
     "who've": 'who have',
     "why'd": 'why did',
     "why're": 'why are',
     "why's": 'why isn',
     "willn't": 'will not',
     "won't": 'will not',
     'wonnot': 'will not',
     "would've": 'would have',
     "wouldn't": 'would not',
     "wouldn't've": 'would not have',
     "y'all'd've": 'you all would have',
     "y'all'd'n've": 'you all would not have',
     "y'all're": 'you all are',
     "you'd": 'you would',
     "you'll": 'you will',
     "you're": 'you are',
     "you've": 'you have',
     "'re": 'are',
     "u": 'you',
     "ur": 'your',
     "n": "and"
    }
