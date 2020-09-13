'''
    This Module impliments functions to fetch
    tweets by Tweet Id. 

    Requirements:- Tweepy
'''
import time

from pandas import DataFrame
import tweepy
import prepare_tweet_ids

from etl.definations_configurations import API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET

# TODO: Implement LOG 


class FetchTweets:
    
    API_KEY = "ZXbDc8BX194cWy79DmKIspDfb"
    API_KEY_SECRET = "CZAe4TVFlUmNPgABBLOBEWgvI4d9THA5IIhxxghcJwGW4clkC7"

    ACCESS_TOKEN = "111951374-TNuHwvgSs4Fg3eeFaoXLheQMfNiAlt2ODZqHmRS7"
    ACCESS_TOKEN_SECRET = "0IjrCabolJocTFLqvioS2nUAIqrHL5SiYs58Dy3INreHT"
    
    
    def __init__(self, api_key = API_KEY, api_key_secret=API_KEY_SECRET,
                 access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET):
        '''
            Initialize the twitter API Object
        '''
        self.api_key = api_key
        self.api_key_secret = api_key_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

        self.auth = tweepy.OAuthHandler(api_key_secret, api_key)
        self.auth.set_access_token(access_token_secret, access_token)

        self.api = tweepy.API(self.auth)


    def fetch_tweet_by_id(self, Id):

        '''
            Fetch a single tweet by ID.
            Returns text of the tweet.
        '''

        try:
            tweet = self.api.get_status(Id).text
            # return tweet
        except Exception as e:
            tweet = None
            print(e)
            # TODO: Logging needed

        return tweet


    def get_labelled_tweet_data(self):
        '''
            Obtain tweet ids and labels
            Use tweet ids to query Twitter APi for tweets
            
            Return: tweetid, tweet, label as a DataFrame
        '''
        # 'row'--> ['tweet_id', 'label']
        ids_labels = prepare_tweet_ids.get_tweet_ids_labels()

        df = DataFrame(ids_labels)
        # df = df.iloc[2000:2030,:] # Test run

        
        tweets = []
        
        
        # fetch the tweets
        # makes twitter api call
        # 300 every 15 minutes
        # due to API restrictions
        for i, Id in enumerate(df['tweet_id']):

            if i+1 >= 300 and i+1 % 300 == 0:
                time.sleep(15*60) # WAIT FOR 15 MINUTES 
            
            tweets.append(self.fetch_tweet_by_id(Id))


        df['tweet'] = tweets

        df = df[['tweet_id', 'tweet', 'label']]

        return df


if __name__ == "__main__":

    # do not call in Standalone
    pass
