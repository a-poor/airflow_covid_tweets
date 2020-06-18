
import os
import json
import time
import dotenv
import tweepy
import sqlite3

import logging



def main():
    queries = [
        "coronavirus",
        "covid19"
    ]
    count = 500
    db_path = "/home/apoor/Desktop/code/Python/twitter_covid/covid_tweets.db"
    schema = """
    CREATE TABLE IF NOT EXISTS "CovidTweets" (
        tweet_id TEXT PRIMARY KEY,
        created_at TEXT,
        tweet_json TEXT
    );"""
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute(schema)
    # Load in the twitter api data
    dotenv.load_dotenv(".env")
    API_KEY = os.environ.get("API_KEY")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
    ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")
    auth = tweepy.OAuthHandler(
        API_KEY,API_SECRET_KEY)
    auth.set_access_token(
        ACCESS_TOKEN,
        ACCESS_TOKEN_SECRET)
    api = tweepy.API(
        auth,
        wait_on_rate_limit=True)
    # print("Starting...")
    successfull = 0
    errors = 0
    for query in queries:
        for search_results in tweepy.Cursor(
            api.search,
            q=query,
            count=count,
            lang='en'
            ).iterator:
            for tweet in search_results:
                try:
                    c.execute("""
                    INSERT INTO CovidTweets (
                        tweet_id,
                        created_at,
                        tweet_json
                    ) VALUES (
                        ?,?,?
                    );""",(
                        str(tweet.id),
                        str(tweet.created_at),
                        str(tweet._json)
                    ))
                except tweepy.RateLimitError:
                    logging.error("Got rate limit error. Sleeping for 10 seconds...")
                    time.sleep(10)
                    errors += 1
                except Exception as e:
                    logging.error("ERROR Storing Tweet: " + str(e))
                    # print("Tweet:",tweet)
                    db.rollback()
                    errors += 1
                else:
                    db.commit()
                    successfull += 1
                # if (successfull+errors) % 1000 == 0: 
                #     print("Successfull %d | Errors: %d" % (successfull,errors))
        time.sleep(10)
    logging.info("Done. Number successful: %d | Number errors: %d" % (successfull,errors))
    return




