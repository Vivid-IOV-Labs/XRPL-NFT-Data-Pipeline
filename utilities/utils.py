import datetime
import json
from io import StringIO
from typing import List

import aioboto3
from .config import Config
import boto3
import numpy as np
import pandas as pd
import tweepy


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


def twitter_pics(prof_img):
    try:
        return (
            prof_img.replace("normal", "400x400"),
            prof_img.replace("normal", "1500x500"),
        )
    except Exception as e:
        print(e)
        return "", ""


def file_to_time(t):
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
    first_record = df.iloc[0].to_dict()
    last_record = df.iloc[-1].to_dict()
    current_value = last_record['y']
    day_ago = df.loc[df["x"] == t - 24 * 60 * 60]["y"]
    week_ago = df.loc[df["x"] == t - 7 * 24 * 60 * 60]["y"]
    first_value = first_record['y'] # Currently month ago
    t0 = float(current_value)
    t1 = float(day_ago)
    t2 = float(week_ago) if not week_ago.empty else 0.0
    t3 = float(first_value)

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
            "all": {
                "valueDifference": t0 - t3,
                "percentageChange": (t0 - t3) / (t3 * 100) if t3 else 0.01,
                "directionOfChange": int(np.sign(t0 - t3)),
            },
        },
    }
    return json.dumps(pct_dic)


def get_s3_resource(config: Config):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    return s3


def write_df(df: pd.DataFrame, config: Config, path: str, file_type: str, **kwargs):
    if config.ENVIRONMENT == "LOCAL":
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
        s3 = get_s3_resource(config)
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
