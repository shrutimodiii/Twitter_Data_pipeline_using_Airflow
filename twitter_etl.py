import pandas as pd
from datetime import datetime
import tweepy
import s3fs
import json

def run_twitter_etl():
  access_token = "HJQKhfuXiYS2WBMLloVXK0Z0t"
  access_token_secret = "0VvOrpDLB4aSPfgU7CyKLoQH6VAH9vlvnS1uQ0XREVFNSVoq2k"
  consumer_key = "1650030855660916741-eiYeRfdV0vcaEBk6JplxiXlvFeJJ4o"
  consumer_secret = "PbEEbGdZ6OeVUnWIF2O97NZDMughISuX7wkJJ3dztdpEc"

  #Twitter Authentication
  auth = tweepy.OAuthHandler(access_token, access_token_secret)
  auth.set_access_token(consumer_key, consumer_secret)

  # Creating an API object 
  api = tweepy.API(auth)

  tweets = api.user_timeline(screen_name = '@elonmusk',
                            count = 200,
                            include_rts = False,
                            tweet_mode = 'extended')
  refined_tweet = []
  for x in tweets:
    text = x._json["full_text"]
    filter_tweet = {"user": x.user.screen_name,
                    'text': text,
                    'favorite_count' : x.favorite_count,
                    'retweet_count' : x.retweet_count,
                    'created_at' : x.created_at}
    refined_tweet.append(filter_tweet)

  df = pd.DataFrame(refined_tweet)
  df.to_csv("s3://shruti-airflow-bucket/twitter_refined_data.csv")