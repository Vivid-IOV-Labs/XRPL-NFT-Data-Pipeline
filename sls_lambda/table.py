import datetime
import logging
from io import BytesIO, StringIO

import numpy as np
import pandas as pd

from utilities import to_snake_case, twitter_pics

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
        await self.writer.write_df(issuers_df, "xls20/latest/NFT_Collections_Profile.json", "json")

        current_time = datetime.datetime.utcnow()
        current = current_time.strftime("%Y-%m-%d-%H")
        previous = (
            datetime.datetime.strptime(current, "%Y-%m-%d-%H")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d-%H")
        df_current = pd.read_csv(f"s3://{config.RAW_DUMP_BUCKET}/{current}.csv")
        df_current.columns = [to_snake_case(x) for x in df_current.columns]
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
            validate='m:m'
        )
        df["promoted"] = "false"

        df["id"] = df.index + 1
        df = df[df["name"].notna()]

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
        df = df[df["issuer"].isin(self.factory.supported_issuers)]
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

        tweets_df = pd.read_csv(f"s3://{config.DATA_DUMP_BUCKET}/xls20/latest/tweets.csv")
        tweets_df["twitter"] = tweets_df["user_name"]
        profile_img_df = tweets_df[["twitter", "profile_image_url"]].drop_duplicates()

        seven_days_ago = current_time - datetime.timedelta(days=7)
        one_day_ago = current_time - datetime.timedelta(days=1)
        eight_days_ago = current_time - datetime.timedelta(days=8)
        tweets_current = tweets_df[["twitter", "tweets", "retweets", "likes"]].loc[
            (tweets_df["timestamp"] <= current_time.timestamp()) &
            (tweets_df["timestamp"] >= seven_days_ago.timestamp())
        ] # 7 days back
        tweets_previous = tweets_df[["twitter", "tweets", "retweets", "likes"]].loc[
            (tweets_df["timestamp"] <= one_day_ago.timestamp()) &
            (tweets_df["timestamp"] >= eight_days_ago.timestamp())
        ] # 7 days back starting from the previous day
        tweets_previous.columns = ["twitter", "tweets_previous", "retweets_previous", "likes_previous"]

        sum_total = tweets_current.groupby("twitter").sum().reset_index(level=0)  # noqa
        sum_previous = tweets_previous.groupby("twitter").sum().reset_index(level=0)

        df["twitter_new"] = df["twitter"].str.lower()
        sum_total["twitter_new"] = sum_total["twitter"].str.lower()
        sum_previous["twitter_new"] = sum_previous["twitter"].str.lower()
        profile_img_df["twitter_new"] = profile_img_df["twitter"].str.lower()
        df = pd.merge(df, sum_total, on="twitter_new", how="left")
        df = pd.merge(df, sum_previous, on="twitter_new", how="left")
        df = pd.merge(df, profile_img_df, on="twitter_new", how="left")

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
        df["re_tweets"] = df["retweets"]
        df["logo_url"], df["banner_url"] = zip(*df["profile_image_url"].apply(twitter_pics))

        df["project"] = df[
            ["issuer", "logo_url", "banner_url", "name", "promoted"]
        ].apply(lambda x: x.to_json(), axis=1)
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
