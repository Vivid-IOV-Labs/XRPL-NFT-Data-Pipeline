import datetime
import logging
import asyncio
import time
from io import BytesIO
from typing import List, Dict

import pandas as pd

from utilities import (cap1, file_to_time, get_pct, TwitterClient, chunks)

from .base import BaseLambdaRunner

logger = logging.getLogger("app_log")


class TwitterDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("data")
        self.twitter_client = TwitterClient(factory.config)

    def _get_tweets_df(self, user_name_list: List[str]) -> pd.DataFrame:
        """
        Fetch tracked issuer tweets for the past 7 days and create a pandas dataframe
        :param user_name_list: List of the usernames of the tracked issuers on twitter
        :return: pandas dataframe
        """
        twitter_users = self.twitter_client.get_users_by_username(user_name_list)
        tweets_list = []
        columns = ["timestamp", "user_name", "tweets", "retweets", "likes", "social_activity", "profile_image_url"]
        # Twitter Allows 10 requests every 15 minutes
        completed = 0
        for twitter_user_chunk in chunks(twitter_users, 10):
            for user in twitter_user_chunk:
                tweets = self.twitter_client.get_user_timeline_n_days_ago(int(user["id"]), 8)
                for tweet in tweets:
                    retweet_count = tweet["public_metrics"]["retweet_count"]
                    like_count = tweet["public_metrics"]["like_count"]
                    tweets_list.append([
                        datetime.datetime.strptime(tweet["created_at"], '%Y-%m-%dT%H:%M:%S.000Z').timestamp(),
                        user["username"], 1,
                        retweet_count,
                        like_count,
                        1 + retweet_count + like_count,
                        user["profile_image_url"]
                    ])
            completed += len(twitter_user_chunk)
            if completed == len(twitter_users):
                break
            logger.info("Sleeping for 15 minutes...")
            time.sleep(900)
            logger.info("Resuming new batch...")
        tweets_df = pd.DataFrame(tweets_list, columns=columns)
        return tweets_df

    async def _write_tweets_df_to_storage(self, tweets_df: pd.DataFrame):
        await self.writer.write_df(tweets_df, "xls20/latest/tweets.csv", "csv")

    @staticmethod
    def _get_graph_data(tweets_df: pd.DataFrame, latest_unix: float, dump_col: List[str]) -> Dict:
        current_time = latest_unix
        data = {col: [] for col in dump_col}
        # creates a 7 day aggregate of tweets, retweets, likes and social activities
        while current_time >= latest_unix - 3600 * 24 * 7:
            mask = (tweets_df["timestamp"] > current_time - 3600 * 24 * 7) & (tweets_df["timestamp"] <= current_time)
            time_data = tweets_df[dump_col].loc[mask].sum()
            for col in dump_col:
                data[col].append([current_time, time_data[col]])
            current_time = current_time - 3600
        return data


    async def _run(self):
        config = self.factory.config
        current = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
        latest_date = file_to_time(current)
        latest_unix = latest_date.timestamp()
        an_hour_ago = (latest_date - datetime.timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        dump_col = ["tweets", "retweets", "likes", "social_activity"]

        df = pd.read_csv(f"s3://{config.RAW_DUMP_BUCKET}/{an_hour_ago}.csv")
        df.columns = [cap1(x) for x in df.columns]
        df = df[df["Twitter"].notna()]
        df["Name"] = df["Name"].str.strip()

        tweets_df = self._get_tweets_df(df.Twitter.unique())
        # tweets_df = pd.read_csv(f"s3://{config.DATA_DUMP_BUCKET}/xls20/latest/tweets.csv")
        # write this to s3 bucket
        await self._write_tweets_df_to_storage(tweets_df)

        graph_data = TwitterDump._get_graph_data(tweets_df, latest_unix, dump_col)
        for col in dump_col:
            df = pd.DataFrame(graph_data[col], columns=["x", "y"])
            pct = bytes(get_pct(df, latest_unix).encode("utf-8"))
            pct_buffer = BytesIO()
            pct_buffer.write(pct)

            await self.writer.write_df(df, f"xls20/latest/{col.title()}_Graph.json", "json")
            await self.writer.write_df(df, f"xls20/history/{int(latest_unix)}/{col.title()}_Graph.json", "json")
            await self.writer.write_buffer(f"xls20/latest/{col.title()}_Percentage_Change.json", pct_buffer)
            await self.writer.write_buffer(f"xls20/history/{int(latest_unix)}/{col.title()}_Percentage_Change.json", pct_buffer)

    def run(self) -> None:
        asyncio.run(self._run())
