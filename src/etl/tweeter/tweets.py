
from pandas import DataFrame, concat

import fetch_tweets
import pre_downloaded_tweets as pdt

class Tweets:
    '''
        Contains a pipeline() method that fetches tweets
        from both the sources.

        Returns: Concatenated tweets from both the sources.
    '''

    def pipeline(self):
        
        # 1st tweet source
        # ['tweet','label']
        # list of named tuples
        df_1 = DataFrame(pdt.get_pre_downloaded_tweets_labels())

        # 2nd and 3rd tweet sources
        # ['tweet_id', 'tweet', 'label']
        # as a dataframe
        df_2 = fetch_tweets.FetchTweets().get_labelled_tweet_data()

        tweets_df = concat([ df_1,
                             df_2[['tweet', 'label']]
                           ]
                          )

        return tweets_df


if __name__ == "__main__":
    
   write_file = os.path.join(TRANSFORMED_DATA_DIR, 'tweeter_data.csv')
    
   data = Tweets().pipeline()
   data.to_csv(write_file, index=False)
