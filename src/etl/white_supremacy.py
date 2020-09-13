'''
      1.  ############ annotations_metadata.csv ##############
         file_id, user_id, subforum_id, num_contexts, label

                pick - file_id and label
      
      2. ########### Read All files ###########
         go to all file directory 
             and 
        collect texts against the file_ids


'''

import os

import collections
from pandas import read_csv as read_csv
from pandas import DataFrame
from pandas import concat, merge

from definitions import DATA_DIR, TRANSFORMED_DATA_DIR

from etl.definations_configurations import ABUSE, NO_ABUSE


class WhiteSupremacy:

    def get_fileId_label(self):
        '''
            Read the file and get Id and Label of the data
        '''

        path = os.path.join(DATA_DIR, "10 - hate-speech-dataset-master")
        
        file_data = read_csv(path+r'\annotations_metadata.csv')
        file_data = file_data.loc[:, ['file_id', 'label']]
        
        # unique labels --> ['noHate' 'hate' 'idk/skip' 'relation']

        file_data = file_data[(file_data.label == 'hate') | (file_data.label == 'noHate')]

        # transform the labels as needed
        file_data.label = file_data.label.apply(lambda x: ABUSE if x.strip().lower() == 'hate' else NO_ABUSE)

        return file_data # ['file_id', 'label']


    def get_white_supremiest_data(self):
        
        '''
            get the comment text
        '''
        file_pattern = '*.txt'
        file_path = DATA_DIR + r"\10 - hate-speech-dataset-master\all_files"
        
        row  = collections.namedtuple('row', ['file_id', 'text'])
        texts = []
        
        id_label_frame = get_fileId_label()

        # id_label_frame = id_label_frame.head(5)
        for file_id in id_label_frame['file_id']:
            
            with open(os.path.join(file_path,file_id+'.txt'), 'r', encoding='utf-8') as file:
                # id_label_frame[file_id,'text'] = file.read().splitlines()
                texts.append(row(file_id=file_id, text=file.read().splitlines()))

        text_df = DataFrame(texts)
        # return id_label_frame

        text_df = merge(id_label_frame, text_df, how='inner', on='file_id')

        text_df = text_df[['text', 'label']]

        return text_df


if __name__ == "__main__":

    write_file = os.path.join(TRANSFORMED_DATA_DIR, 'white_supremist_data.csv')
    
    data = WhiteSupremacy().get_white_supremiest_data()
    data.to_csv(write_file, index=False)
