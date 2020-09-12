import tweets
import white_supremacy
import wkpd_personal_attacks
import toxic_comments
import fb_hate_speech

write_path = r"processed data/"

tweeter_data = tweets.mine_tweets()
if not os.path.isfile(write_path+'tweeter_data.csv'):
    tweeter_data.to_csv(write_path+'tweeter_data.csv', index=False)
else: # else it exists so append without writing the header
    tweeter_data.to_csv(write_path+'tweeter_data.csv', mode='a', header=False, index=False)


data = white_supremacy.get_white_supremiest_data()
data.to_csv(write_path+'white_supremist_data.csv', index=False)


data = wkpd_personal_attacks.get_wikipedia_personal_attacks()
data.to_csv(write_path+'wikipedia_personal_attacks.csv', index=False)

toxic_comments = toxic_comments.get_toxic_comments()
toxic_comments.to_csv(write_path+'toxic_comments.csv', index=False)

data = fb_hate_speech.get_fb_hate_speech()
data.to_csv(write_path+'facebook_hate_speech_translated.csv', index=False)

data = unintended_toxic_comments.get_unintended_toxic_comments()
data.to_csv(write_path+'unintended_toxic_comments.csv', index=False)
