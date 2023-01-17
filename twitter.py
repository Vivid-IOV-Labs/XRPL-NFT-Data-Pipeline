from io import StringIO
import datetime

import pandas as pd
# import snscrape.modules.twitter as twitter_scrapper

from config import Config
from utils import cap1, file_to_time, get_pct, get_s3_resource, write_df, get_last_n_tweets


def twitter():
    # files = sorted(
    #     [f"{(datetime.datetime.utcnow() - datetime.timedelta(i)).strftime('%Y-%m-%d-%H')}.csv" for i in range(30)])

    # nft_list = open("data/text/list_nft.txt").read().splitlines()

    current = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')
    latest_date = file_to_time(current)
    latest_unix = latest_date.timestamp()
    tweets_list = []
    t_col = ["Tweets", "Retweets", "Likes", "Social_Activity"]

    df = pd.read_csv(f"s3://{Config.RAW_DUMP_BUCKET}/{current}.csv")
    df.columns = [cap1(x) for x in df.columns]
    df = df[df["Twitter"].notna()]
    df["Name"] = df["Name"].str.strip()

    #name_to_twitter = dict(zip(df.Name, df.Twitter))

    twitter_list = df.Twitter.unique()
    api_key = Config.TWITTER_API_KEY
    api_secret = Config.TWITTER_API_SECRET
    for name in twitter_list:
        tweets = get_last_n_tweets(name, api_key, api_secret)
        for tweet in tweets:
            diff = (latest_date - tweet.created_at.replace(tzinfo=None)).total_seconds()
            if diff < 0:
                continue
            if diff >= 3600 * 24 * 14:
                break
            tweets_list.append(
                [
                    tweet.created_at.timestamp(),
                    name,
                    1,
                    tweet.retweet_count,
                    tweet.favorite_count,
                    1 + tweet.retweet_count + tweet.favorite_count,
                ]
            )
    tweets = pd.DataFrame(tweets_list, columns=["Date", "Name"] + t_col)
    t = latest_unix
    dic = {}
    for col in t_col:
        dic[col] = []
    while t >= latest_unix - 3600 * 24 * 7:
        mask = (tweets["Date"] > t - 3600 * 24 * 7) & (tweets["Date"] <= t)
        s = tweets[t_col].loc[mask].sum()
        for col in t_col:
            dic[col].append([t, s[col]])
        t = t - 3600
    for col in t_col:
        df = pd.DataFrame(dic[col], columns=["x", "y"])
        pct = get_pct(df, latest_unix)
        if Config.ENVIRONMENT == "PROD":
            write_df(df, f"latest/{col}_Graph.json", "json", bucket=Config.DATA_DUMP_BUCKET)
            write_df(df, f"history/{latest_unix}/{col}_Graph.json", "json", bucket=Config.DATA_DUMP_BUCKET)
            buffer = StringIO()
            buffer.write(pct)
            s3 = get_s3_resource()
            s3.Object(Config.DATA_DUMP_BUCKET, f"history/{latest_unix}/{col}_Percentage_Change.json").put(
                Body=buffer.getvalue()
            )
            s3.Object(Config.DATA_DUMP_BUCKET, f"latest/{col}_Percentage_Change.json").put(Body=buffer.getvalue())
        else:
            write_df(df, f"data/json_dumps/{col}_Graph.json", "json")
            with open(f"data/json_dumps/{col}_Percentage_Change.json", "w") as file:
                file.write(pct)
    return {"statuscode": 200}
