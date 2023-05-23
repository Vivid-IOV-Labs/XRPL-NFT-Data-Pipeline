import datetime
import json
from io import StringIO
from typing import List

import aioboto3
import boto3
import numpy as np
import pandas as pd
import snscrape.modules.twitter as twitter_scrapper
import tweepy

from .config import Config


def chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_issuer_taxons(issuer, environment, bucket, access_key, secret_key):
    if environment == "LOCAL":
        taxons = json.load(open("data/nfts/taxon.json", "r"))
        return [taxon["taxons"] for taxon in taxons if taxon["issuer"] == issuer][0]
    else:
        client = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        result = client.get_object(Bucket=bucket, Key="taxon.json")
        text = result["Body"].read()
        taxons = json.loads(text.decode())
        return [taxon["taxons"] for taxon in taxons if taxon["issuer"] == issuer][0]


def fetch_issuer_tokens(issuer, environment, bucket, access_key, secret_key):
    last_hour = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
    # last_hour = "2022-12-11-13"
    if environment == "LOCAL":
        last_hour = "2022-12-11-13"
        data = json.load(open(f"data/nfts/{last_hour}/{issuer}.json", "r"))
        return data["nfts"]
    else:
        client = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        result = client.get_object(Bucket=bucket, Key=f"{last_hour}/{issuer}.json")

        text = result["Body"].read()
        return json.loads(text.decode())["nfts"]


def fetch_dumped_token_prices(issuer, config):
    last_hour = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    my_bucket = s3.Bucket(config.PRICE_DUMP_BUCKET)
    return [
        obj.key
        for obj in my_bucket.objects.filter(Prefix=f"{last_hour}/{issuer}/tokens/")
    ]


def fetch_dumped_taxon_prices(issuer, config):
    last_hour = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    my_bucket = s3.Bucket(config.PRICE_DUMP_BUCKET)
    return [
        obj.key
        for obj in my_bucket.objects.filter(Prefix=f"{last_hour}/{issuer}/taxons/")
    ]


async def read_json(bucket, key, config):
    session = aioboto3.Session(
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    async with session.client("s3") as s3:
        try:
            res = await s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(f"Error Reading JSON @ {key} in bucket: {bucket}\nError: {e}")
            return None
        body = res["Body"]
        data = await body.read()
        return json.loads(data)


def to_snake_case(col: str):
    return "_".join([x.lower() for x in col.split("_")])


def twitter_pics(name):
    try:
        pic = twitter_scrapper.TwitterUserScraper(name)._get_entity()  # noqa
        return (
            pic.profileImageUrl.replace("normal", "400x400"),
            pic.profileBannerUrl + "/1500x500",
        )
    except Exception as e:
        print(e)
        return "", ""


def file_to_time(t):
    # print(t)
    # print(t[:4], t[5:7], t[8:10], t[11:13])
    return datetime.datetime(int(t[:4]), int(t[5:7]), int(t[8:10]), int(t[11:13]))


def cap1(col: str):
    return "_".join([x.title() for x in col.split("_")])


def read_df(filename):
    df = pd.read_csv(filename)
    df.columns = [cap1(x) for x in df.columns]
    df = df[df["Twitter"].notna()]
    df["Name"] = df["Name"].str.strip()
    return df


def get_pct(df, t):
    first_record = df.head(1).to_dict(orient="list")
    first_unix = first_record["x"][0]
    s0 = df.loc[df["x"] == t]["y"]
    s1 = df.loc[df["x"] == t - 24 * 60 * 60]["y"]
    s2 = df.loc[df["x"] == t - 7 * 24 * 60 * 60]["y"]
    s3 = df.loc[df["x"] == first_unix]["y"]

    t0 = float(s0) if s0.any() else 0
    t1 = float(s1) if s1.any() else 0
    t2 = float(s2) if s2.any() else 0
    t3 = float(s3) if s3.any() else 0

    pct_dic = {
        "currentValue": t0,
        "percentageChanges": {
            "day": {
                "valueDifference": t0 - t1,
                "percentageChange": (t0 - t1) / (t1 * 100) if t1 else 0.01,
                "directionOfChange": int(np.sign(t0 - t1)),
            },
            "week": {
                "valueDifference": t0 - t2,
                "percentageChange": (t0 - t2) / (t2 * 100) if t2 else 0.01,
                "directionOfChange": int(np.sign(t0 - t2)),
            },
            "month": {
                "valueDifference": t0 - t3,
                "percentageChange": (t0 - t3) / (t3 * 100) if t3 else 0.01,
                "directionOfChange": int(np.sign(t0 - t3)),
            },
        },
    }
    return json.dumps(pct_dic)


def get_s3_resource():
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=Config.ACCESS_KEY_ID,
        aws_secret_access_key=Config.SECRET_ACCESS_KEY,
    )
    return s3


def write_df(df: pd.DataFrame, path: str, file_type: str, **kwargs):
    if Config.ENVIRONMENT == "LOCAL":
        if path.startswith("data") is False:
            path = (
                f"data/csv_dumps/{path}"
                if file_type == "csv"
                else f"data/json_dumps/{path}"
            )
        if file_type == "csv":
            df.to_csv(path, index=False)
        elif file_type == "json":
            df.to_json(path, indent=4, orient="records")
    else:
        s3 = get_s3_resource()
        buffer = StringIO()
        if file_type == "csv":
            df.to_csv(buffer, index=False)  # noqa
        if file_type == "json":
            df.to_json(buffer, indent=4, orient="records")  # noqa
        s3.Object(kwargs["bucket"], path).put(Body=buffer.getvalue())


def get_day_df(df: pd.DataFrame, max_points: int):
    return df[-max_points:]


def get_weekly_df(df: pd.DataFrame, max_points: int):
    return df[-max_points:]


def get_monthly_df(df: pd.DataFrame, max_points: int):
    last_item = df.tail(1).to_dict(orient="list")
    current_time, current_value = last_item["x"][0], last_item["y"][0]
    points = [{"x": current_time, "y": current_value}]
    points_traversed = 1
    while points_traversed < max_points:
        try:
            to_append = df[df["x"] == current_time - 3600].to_dict(orient="list")
            points.append({"x": to_append["x"][0], "y": to_append["y"][0]})
        except Exception as e:
            print(f"Error extracting value for timestamp {current_time - 3600}: {e}")
            pass
        current_time -= 3600
        points_traversed += 1
    return pd.DataFrame(points[::-1])


def get_last_n_tweets(user_name, api_key, secret_key, n=100):
    auth = tweepy.OAuth2AppHandler(api_key, secret_key)
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name=user_name, count=n)
    return tweets


async def execute_sql_file(conn, sql_file):
    with open(sql_file, "r") as f:
        sql = f.read()
    async with conn.cursor() as cur:
        result = await cur.execute(sql)
        return result
