import pandas as pd
import numpy as np
import datetime
from io import StringIO, BytesIO

from utils import to_snake_case, twitter_pics, get_last_n_tweets
from config import Config
from writers import AsyncS3FileWriter
from main import factory
from utils import write_df

# import snscrape.modules.twitter as twitter_scrapper
# import tweepy

async def table():

    # Dump Collections Profile
    issuers_df = factory.issuers_df
    write_df(issuers_df, f"xls20/latest/NFT_Collections_Profile.json", "json", bucket=Config.DATA_DUMP_BUCKET)

    current_time = datetime.datetime.utcnow()
    # day_ago = current_time - datetime.timedelta(days=1)
    current = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')
    previous = (datetime.datetime.strptime(current, '%Y-%m-%d-%H') - datetime.timedelta(days=1)).strftime('%Y-%m-%d-%H')
    df_previous = pd.read_csv(f"s3://{Config.RAW_DUMP_BUCKET}/{previous}.csv")
    df_previous.columns = [to_snake_case(col) for col in df_previous.columns]
    df_current = pd.read_csv(f"s3://{Config.RAW_DUMP_BUCKET}/{current}.csv")
    df_current.columns = [to_snake_case(x) for x in df_current.columns]
    # df_current = df_current[df_current["twitter"].notna()]
    df_current["name"] = df_current["name"].str.strip()

    df_current["total_supply"] = df_current["supply"]
    df_current["circulating_supply"] = df_current["circulation"]

    df = pd.merge(df_current, df_previous, on="issuer", how="outer", suffixes=("", "_previous"))

    df["promoted"] = "false"

    df["id"] = df.index + 1
    df = df[df["name"].notna()]
    df["logo_url"], df["banner_url"] = zip(*df["twitter"].apply(twitter_pics))

    df["project"] = df[["issuer", "logo_url", "banner_url", "name", "promoted"]].apply(
        lambda x: x.to_json(), axis=1
    )

    df["pricexrp"] = df["pricexrp"]/1000000
    df["pricexrp_previous"] = df["pricexrp_previous"]/1000000
    df["market_cap"] = df["market_cap"]/1000000
    df["market_cap_previous"] = df["market_cap_previous"]/1000000

    df["value"] = df["holder_count"].fillna(0.0).astype(int)
    df["direction_of_change"] = np.sign(df["holder_count"] - df["holder_count_previous"]).fillna(0.0).astype(int)
    df["holder_count"] = df[["value", "direction_of_change"]].apply(lambda x: x.to_json(), axis=1)

    df["value"] = df["pricexrp"]
    df["percentage_change"] = round(abs((df["pricexrp"] - df["pricexrp_previous"]) / df["pricexrp_previous"] * 100), 2)
    df["direction_of_change"] = np.sign(df["pricexrp"] - df["pricexrp_previous"]).fillna(0.0).astype(int)
    df["price_xrp"] = df[["value", "percentage_change", "direction_of_change"]].apply(lambda x: x.to_json(), axis=1)

    df["market_cap"] = df["market_cap"].astype(float)
    df = df.sort_values(by=["market_cap"], ascending=False)
    df = df.reset_index()
    df["rank"] = df.index + 1
    df["value"] = df["market_cap"]
    df["direction_of_change"] = np.sign(df["market_cap"] - df["market_cap_previous"]).fillna(0.0).astype(int)
    df["market_cap"] = df[["value", "direction_of_change"]].apply(lambda x: x.to_json(), axis=1)

    tweets_list = []
    tweets_list_previous = []
    twitter_list = df.twitter.unique()
    api_key = Config.TWITTER_API_KEY
    api_secret = Config.TWITTER_API_SECRET
    # print(twitter_list)
    for name in twitter_list:
        # print(name)
        if type(name) == float:
            continue
        tweets = get_last_n_tweets(name, api_key, api_secret)
        for tweet in tweets:
            diff = (current_time - tweet.created_at.replace(tzinfo=None)).total_seconds()
            if diff < 0:
                continue
            if diff >= 3600 * 24 * 8:
                break
            if diff < 3600 * 24 * 7:
                tweets_list.append([name, 1, tweet.retweet_count, tweet.favorite_count])
            if diff >= 3600 * 24 * 1 and diff < 3600 * 24 * 8:  # noqa
                tweets_list_previous.append([name, 1, tweet.retweet_count, tweet.favorite_count])

    tweets = pd.DataFrame(tweets_list, columns=["twitter", "tweets", "retweets", "likes"])
    sum = tweets.groupby("twitter").sum().reset_index(level=0)  # noqa

    tweets_previous = pd.DataFrame(
        tweets_list, columns=["twitter", "tweets_previous", "retweets_previous", "likes_previous"]
    )
    sum_previous = tweets_previous.groupby("twitter").sum().reset_index(level=0)
    df = pd.merge(df, sum, on="twitter", how="outer")
    df = pd.merge(df, sum_previous, on="twitter", how="outer")
    df["re_tweets"] = df["retweets"]

    df["value"] = (df["tweets"] + df["retweets"] + df["likes"]).fillna(0.0).astype(int)
    df["percentage"] = round(df["value"] / df["value"].max(), 2)
    df["social_previous"] = (
        (df["tweets_previous"] + df["retweets_previous"] + df["likes_previous"]).fillna(0.0).astype(int)
    )

    df["direction_of_change"] = np.sign(df["value"] - df["social_previous"]).fillna(0.0).astype(int)
    df["social_activity"] = df[["value", "percentage", "direction_of_change"]].apply(lambda x: x.to_json(), axis=1)
    output = df[
        [
            "id",
            "rank",
            "project",
            "holder_count",
            "social_activity",
            "price_xrp",
            "market_cap",
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
    new_b.write(bytes(content, 'utf-8'))
    await AsyncS3FileWriter(
        Config.DATA_DUMP_BUCKET
    ).write_buffer("xls20/latest/NFT_Collections_Table.json", new_b)
