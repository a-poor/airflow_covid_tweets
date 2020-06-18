
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

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
    dotenv.load_dotenv("/home/apoor/Desktop/code/Python/twitter_covid/.env")
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



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'CovidTwitterScrape',
    default_args=default_args,
    description='Downloading tweets about COVID-19 for later processing.',
    schedule_interval=timedelta(hours=2),
)


twitter_operator = PythonOperator(
    task_id="ScrapeTwitter",
    provide_context=False,
    python_callable=main,
    dag=dag
)

backup_db = BashOperator(
    task_id="Backup database",
    bash_command=r'cp /home/apoor/Desktop/code/Python/twitter_covid/covid_tweets.db $(date +"/home/apoor/Desktop/code/Python/twitter_covid/covid_tweets_%Y%m%d.%H%M%S.db")'
)

twitter_operator >> backup_db
