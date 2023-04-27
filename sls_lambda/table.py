import datetime
import logging
from io import BytesIO, StringIO

import numpy as np
import pandas as pd

from utilities import get_last_n_tweets, to_snake_case, twitter_pics, write_df

from .base import BaseLambdaRunner

logger = logging.getLogger("app_log")


class TableDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("data")

    async def run(self):
        # Dump Collections Profile
        issuers_df = self.factory.issuers_df
        config = self.factory.config

        write_df(
            issuers_df,
            f"xls20/latest/NFT_Collections_Profile.json",
            "json",
            bucket=config.DATA_DUMP_BUCKET,
        )

        current_time = datetime.datetime.utcnow()
        # day_ago = current_time - datetime.timedelta(days=1)
        current = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
        previous = (
            datetime.datetime.strptime(current, "%Y-%m-%d-%H")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d-%H")
        df_current = pd.read_csv(f"s3://{config.RAW_DUMP_BUCKET}/{current}.csv")
        df_current.columns = [to_snake_case(x) for x in df_current.columns]
        # df_current = df_current[df_current["twitter"].notna()]
        df_current["name"] = df_current["name"].str.strip()

        df_current["total_supply"] = df_current["supply"]
        df_current["circulating_supply"] = df_current["circulation"]
        df_previous = df_current.copy(deep=True)
        try:
            df_previous = pd.read_csv(f"s3://{config.RAW_DUMP_BUCKET}/{previous}.csv")
            df_previous.columns = [to_snake_case(col) for col in df_previous.columns]
        except FileNotFoundError:
            logger.error("Previous Day CSV File Not Found")
        df = pd.merge(
            df_current,
            df_previous,
            on="issuer",
            how="outer",
            suffixes=("", "_previous"),
        )

        df["promoted"] = "false"

        df["id"] = df.index + 1
        df = df[df["name"].notna()]
        df["logo_url"], df["banner_url"] = zip(*df["twitter"].apply(twitter_pics))

        df["project"] = df[
            ["issuer", "logo_url", "banner_url", "name", "promoted"]
        ].apply(lambda x: x.to_json(), axis=1)

        df[
            [
                "pricexrp",
                "pricexrp_previous",
                "floor_price_xrp",
                "floor_price_xrp_previous",
                "mid_price_xrp",
                "mid_price_xrp_previous",
                "max_buy_offer_xrp",
                "max_buy_offer_xrp_previous",
                "market_cap",
                "market_cap_previous",
                "volume"
            ]
        ] = (
            df[
                [
                    "pricexrp",
                    "pricexrp_previous",
                    "floor_price_xrp",
                    "floor_price_xrp_previous",
                    "mid_price_xrp",
                    "mid_price_xrp_previous",
                    "max_buy_offer_xrp",
                    "max_buy_offer_xrp_previous",
                    "market_cap",
                    "market_cap_previous",
                    "volume"
                ]
            ]
            / 1000000
        )

        df["value"] = df["holder_count"].fillna(0.0).astype(int)
        df["direction_of_change"] = (
            np.sign(df["holder_count"] - df["holder_count_previous"])
            .fillna(0.0)
            .astype(int)
        )
        df["holder_count"] = df[["value", "direction_of_change"]].apply(
            lambda x: x.to_json(), axis=1
        )

        price_cols = [
            "pricexrp",
            "floor_price_xrp",
            "mid_price_xrp",
            "max_buy_offer_xrp",
        ]
        for col in price_cols:
            df["value"] = df[col]
            df["percentage_change"] = round(
                abs((df[col] - df[f"{col}_previous"]) / df[f"{col}_previous"] * 100),
                2,
            )
            df["direction_of_change"] = (
                np.sign(df[col] - df[f"{col}_previous"]).fillna(0.0).astype(int)
            )
            df[col] = df[["value", "percentage_change", "direction_of_change"]].apply(
                lambda x: x.to_json(), axis=1
            )

        df["market_cap"] = df["market_cap"].astype(float)
        df = df.sort_values(by=["market_cap"], ascending=False)
        df = df.reset_index()
        df["rank"] = df.index + 1
        df["value"] = df["market_cap"]
        df["direction_of_change"] = (
            np.sign(df["market_cap"] - df["market_cap_previous"])
            .fillna(0.0)
            .astype(int)
        )
        df["market_cap"] = df[["value", "direction_of_change"]].apply(
            lambda x: x.to_json(), axis=1
        )

        tweets_list = []
        tweets_list_previous = []
        twitter_list = df.twitter.unique()
        api_key = config.TWITTER_API_KEY
        api_secret = config.TWITTER_API_SECRET
        for name in twitter_list:
            if type(name) == float:
                continue
            tweets = []
            try:
                tweets = get_last_n_tweets(name, api_key, api_secret)
            except Exception as e:
                logger.error(e)
                continue
            for tweet in tweets:
                diff = (
                    current_time - tweet.created_at.replace(tzinfo=None)
                ).total_seconds()
                if diff < 0:
                    continue
                if diff >= 3600 * 24 * 8:
                    break
                if diff < 3600 * 24 * 7:
                    tweets_list.append(
                        [name, 1, tweet.retweet_count, tweet.favorite_count]
                    )
                if diff >= 3600 * 24 * 1 and diff < 3600 * 24 * 8:  # noqa
                    tweets_list_previous.append(
                        [name, 1, tweet.retweet_count, tweet.favorite_count]
                    )

        tweets = pd.DataFrame(
            tweets_list, columns=["twitter", "tweets", "retweets", "likes"]
        )
        sum = tweets.groupby("twitter").sum().reset_index(level=0)  # noqa

        tweets_previous = pd.DataFrame(
            tweets_list,
            columns=[
                "twitter",
                "tweets_previous",
                "retweets_previous",
                "likes_previous",
            ],
        )
        sum_previous = tweets_previous.groupby("twitter").sum().reset_index(level=0)
        df = pd.merge(df, sum, on="twitter", how="outer")
        df = pd.merge(df, sum_previous, on="twitter", how="outer")
        df["re_tweets"] = df["retweets"]

        df["value"] = (
            (df["tweets"] + df["retweets"] + df["likes"]).fillna(0.0).astype(int)
        )
        df["percentage"] = round(df["value"] / df["value"].max(), 2)
        df["social_previous"] = (
            (df["tweets_previous"] + df["retweets_previous"] + df["likes_previous"])
            .fillna(0.0)
            .astype(int)
        )

        df["direction_of_change"] = (
            np.sign(df["value"] - df["social_previous"]).fillna(0.0).astype(int)
        )
        df["social_activity"] = df[
            ["value", "percentage", "direction_of_change"]
        ].apply(lambda x: x.to_json(), axis=1)
        df["price_xrp"] = df["pricexrp"]
        output = df[
            [
                "id",
                "rank",
                "project",
                "holder_count",
                "tokens_held",
                "social_activity",
                "price_xrp",
                "pricexrp",
                "floor_price_xrp",
                "mid_price_xrp",
                "max_buy_offer_xrp",
                "market_cap",
                "volume",
                "tweets",
                "re_tweets",
                "likes",
                "total_supply",
                "circulating_supply",
            ]
        ]
        buffer = StringIO()
        output.to_json(buffer, orient="records", indent=4)
        content = buffer.getvalue()
        content = (
            content.replace('"{', "{")
            .replace('}"', "}")
            .replace("\\", "")
            .replace('"false"', "false")
            .replace('"true"', "true")
        )
        new_b = BytesIO()
        new_b.write(bytes(content, "utf-8"))
        await self.writer.write_buffer("xls20/latest/NFT_Collections_Table.json", new_b)

    def sync_run(self):
        import asyncio

        asyncio.run(self.run())
