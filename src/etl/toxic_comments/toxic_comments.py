'''
    This module build the main pipeline for toxic comments.
    Pulls data from both the Jigsaw Toxic comment sources.
'''

import os

from pandas import concat

from definitions import TRANSFORMED_DATA_DIR

import toxic_comments_src1, toxic_comments_src2

class ToxicComment:

    def pipeline(self):
        '''
         Simple function to call the two sources
         and return the concatenated data frame
        '''

        try:
            toxic_comments = concat([
                                        toxic_comments_src1.get_unintended_toxic_comments(),
                                        toxic_comments_src2.get_toxic_comments()
                                    ]
                                )

            return toxic_comments

        except Exception as e:
            print(e)
            # TODO: implement Logging framework


if __name__ == "__main__":
    
    write_file = os.path.join(TRANSFORMED_DATA_DIR, 'toxic_comments.csv')

    toxic_comments = ToxicComment().pipeline()
    toxic_comments.to_csv(write_file, index=False)
