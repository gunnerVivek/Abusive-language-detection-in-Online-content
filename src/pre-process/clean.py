import re

import pandas as pd
import unicodedata

import spacy
nlp = spacy.load('en_core_web_md')
from spacy.lang.en.stop_words import STOP_WORDS

from definitions import CONTRACTIONS

data = pd.read_csv(r"E:\repositories\Abusive-language-detection-in-Online-content\cleaned_data\combined.csv", nrows=50)

# data = data.sample(200000)
# print(data.shape)
# data.to_csv('data_1.csv',index=False)

def handle_percent(percent):

    # find-all returns a list
    num = re.findall("\d+", percent)[0] 
    
    if float(num) < 100:
        return 'part'
    else:
        return 'whole'


def get_de_contracted(document):
    
    # document already is str
    words = document.split()
    
    de_contracted_words = []
    
    for word in words:
        c_word = CONTRACTIONS.get(word)
        if c_word:
            de_contracted_words.append(c_word)
        else:
            de_contracted_words.append(word)
            
    return ' '.join(de_contracted_words)


def clean(document, sequence=False):

    '''
        Handles each document (comment) at a time.

        Cleaning STEPS :
        1. handle decimal for apostrophe
        2. decimal emojis --> &#\d+;
        3. accented characters 
        4. replace percent  
        5. eMail
        6. URL
        7. html tags
        8. Mentions
        9. Hashtags
        10. Remove Newline
        11. 137c9c6970afb7fc
        12. de contraction
        13. repeating special chars - !!!
        14. special chars --> if not sequence then remove full stop
        15. sinlge chars
        16. lower case
        17. multiple spaces
        18. Stop words --> if not sequence then remove
        
        a and stop words not for sequence cleaning
    '''

    # handle only floating point values
    document = str(document)

    # 1. Apostrophe    
    document = re.sub(r"&#39;", "'", document)
    
    # 2. Emojis
    document = re.sub(r"&#\d+;", " ", document)
    
    # 3. Accented characters
    document = unicodedata.normalize('NFKD', document).encode('ascii', 'ignore').decode('utf-8', 'ignore')

    # 4. replace percent
    percents = re.findall(r"\b\d+\s*\%\B", document) # find all percentages
    if percents:
        for percent in set(percents):
            document = document.replace(percent, handle_percent(percent))
    
    # 5. eMail
    email = r"[a-zA-Z]+@([a-zA-Z0-9]+\.)+([a-zA-z]){2,3}"
    document = re.sub(email, " ", document)

    # 6.URL
    url = r"(((ht|f)tp(s)?:\/\/)|(www.))([\w-]+\.)+([a-zA-Z])*\/*[^\.\s]*"
    document = re.sub(email, " ", document)

    # 7. HTML tags
    html_re = r"<\/*[a-zA-Z0-9]+\/*>"
    document = re.sub(html_re, " ", document)
    
    # 8. Mentions
    mentions = r"(?<!\S)@\w+"
    document = re.sub(mentions, " ", document)

    # 9. Hashtags
    hashtags = r"(?<!\S)\#[\w#-]+"
    document = re.sub(hashtags, " ", document)

    # 10. Remove new line 
    document = document.replace(r"\\r|\r", ' ')
    document = document.replace(r"\\n|\n", ' ')

    # 11. mix of numbers and chars
    num_char = r"\b(([a-z]+\d+)|(\d+[a-z]+))(\w)+\b"
    document = re.sub(num_char, " ", document)

    # 12. de contraction
    document =  get_de_contracted(document)

    # 13. Repeating special chars
    repeating_special_chars = r"([^\w\s])\1{1,}"
    document = re.sub(repeating_special_chars, " ", document)

    # 14. Special chars and numbers
    if sequence:
        # full stop should not be removed; space.space(remove)
        special_char = r"[^a-zA-Z\.\s]|\s\.\s" 
    else:
        special_char = r"[^a-zA-z\s]" # remove full stops as well
    document = re.sub(special_char, " ", document)

    # 15. Single chars
    single_char = r"\b([^aIi])\s{1,}"
    document = re.sub(single_char, " ",document)

    # 16. Lower case
    document = document.lower()

    # 17. multiple spaces
    document = re.sub(r"\s{2,}|\t", " ", document)

    # 18. Stop words removal
    if not sequence:
        document = ' '.join([w for w in document.split() if w not in STOP_WORDS])

    return document


def clean_data(data, sequence=False):

    data.message = data.message.apply(clean, args=(sequence))

    print(data.head(20))

    print(data.message.loc[3])

if __name__ == "__main__":
    clean_data(data, sequence=False)
    # remove full stop if not sequence model