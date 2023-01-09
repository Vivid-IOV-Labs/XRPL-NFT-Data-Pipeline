import asyncio
from io import StringIO, BytesIO
import aiohttp
import logging
import json

import boto3
import httpx  # noqa
from config import Config
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models.requests import NFTBuyOffers, NFTSellOffers
from xrpl.models.response import ResponseStatus
from writers import LocalFileWriter, AsyncS3FileWriter
from factory import Factory
import snscrape.modules.twitter as twitter_scrapper
from utils import chunks, fetch_issuer_taxons, fetch_issuer_tokens, read_json, to_snake_case, twitter_pics
import pandas as pd  # noqa
import numpy as np
import time
import datetime


logger = logging.getLogger("app_log")
formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)
factory = Factory()


async def get_token_offer(token_id, server):
    """Fetches buy and sell offers for a particular token from the ledger"""
    client = AsyncJsonRpcClient(server)
    buy_offer_request = NFTBuyOffers(nft_id=token_id)
    sell_offer_request = NFTSellOffers(nft_id=token_id)
    buy_offers = await client.request(buy_offer_request)
    sell_offers = await client.request(sell_offer_request)

    offers = []
    buy_offers = buy_offers.result["offers"] if buy_offers.status == ResponseStatus.SUCCESS else []
    sell_offers = sell_offers.result["offers"] if sell_offers.status == ResponseStatus.SUCCESS else []
    buy_offers_amount_arr = [float(offer["amount"]) for offer in buy_offers if type(offer["amount"]) != dict]
    sell_offers_amount_arr = [float(offer["amount"]) for offer in sell_offers if type(offer["amount"]) != dict]
    highest_buy_offer = max(buy_offers_amount_arr) if buy_offers_amount_arr else 0
    lowest_sell_offer = min(sell_offers_amount_arr) if sell_offers_amount_arr else 0
    offers.extend([highest_buy_offer, lowest_sell_offer])
    return offers


async def dump_issuer_nfts(issuer):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://api.xrpldata.com/api/v1/xls20-nfts/issuer/{issuer}') as response:
            content = await response.content.read()
            data = json.loads(content)["data"]
            now = datetime.datetime.utcnow()
            if Config.ENVIRONMENT == "LOCAL":
                LocalFileWriter().write_json(
                    data,
                    f"data/nfts/{now.strftime('%Y-%m-%d-%H')}",
                    f"{issuer}.json"
                )
            else:
                await AsyncS3FileWriter(
                    Config.NFT_DUMP_BUCKET
                ).write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}.json", data)
            return {
                "issuer": issuer, "supply": len(data["nfts"]),
                "circulation": len([token for token in data["nfts"] if token["Owner"] != issuer])
            }


async def get_issuer_taxons(issuer):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://api.xrpldata.com/api/v1/xls20-nfts/taxon/{issuer}') as response:
            content = await response.content.read()
            data = json.loads(content)
            return data["data"]


async def get_taxon_token_offers(issuer, taxon, token_id):
    for server in Config.XRPL_SERVERS:
        try:
            offers = await get_token_offer(token_id, server)
            average = sum(offers)/len(offers) if offers else 0
            return average
        except httpx.RequestError as e:
            logger.error(f"Failed Using {server} with Error {e}: Retrying ...")
            continue
    logger.error(f"Could not get offers from any server for issuer {issuer}, taxon {taxon}, and token {token_id}")
    return 0


async def taxon_offers(taxon, issuer, tokens):
    tokens = [token for token in tokens if token["Taxon"] == taxon]
    logger.info(f"Running for Taxon {taxon} With {len(tokens)} Tokens")
    now = datetime.datetime.utcnow()
    offers = []
    for chunk in chunks(tokens, 500):
        averages = await asyncio.gather(*[get_taxon_token_offers(issuer, taxon, token["NFTokenID"]) for token in chunk])
        offers.extend([average for average in averages if average != 0])
    data = {"taxon": taxon, "issuer": issuer, "average_price": sum(offers)/len(offers) if offers else 0, "floor_price": min(offers) if offers else 0}
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(data, f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}", f"{taxon}.json")
    else:
        await AsyncS3FileWriter(
            Config.PRICE_DUMP_BUCKET
        ).write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/{taxon}.json", data)
    return data["floor_price"]


async def dump_issuers_nfts():
    supported_issuers = factory.supported_issuers
    supply = []
    for chunk in chunks(supported_issuers, 10):
        result = await asyncio.gather(*[dump_issuer_nfts(issuer) for issuer in chunk])
        supply.extend(result)
        time.sleep(60)
    await AsyncS3FileWriter(
        Config.NFT_DUMP_BUCKET
    ).write_json("supply.json", supply)


async def dump_issuers_taxons():
    supported_issuers = factory.supported_issuers
    taxons = []
    for chunk in chunks(supported_issuers, 10):
        chunk_taxons = await asyncio.gather(*[get_issuer_taxons(issuer) for issuer in chunk])
        taxons.extend(chunk_taxons)
        time.sleep(60)
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(taxons, "data/nfts", "taxon.json")
    else:
        await AsyncS3FileWriter(
            Config.NFT_DUMP_BUCKET
        ).write_json("taxon.json", taxons)


async def dump_issuer_taxon_offers(issuer):
    now = datetime.datetime.utcnow()
    taxons = fetch_issuer_taxons(issuer, Config.ENVIRONMENT, Config.NFT_DUMP_BUCKET, Config.ACCESS_KEY_ID, Config.SECRET_ACCESS_KEY)
    tokens = fetch_issuer_tokens(issuer, Config.ENVIRONMENT, Config.NFT_DUMP_BUCKET, Config.ACCESS_KEY_ID, Config.SECRET_ACCESS_KEY)
    logger.info(f"Taxon Count: {len(taxons)}\nToken Count: {len(tokens)}")
    # taxons = taxons[:10]  # temporary
    average_prices = await asyncio.gather(*[taxon_offers(taxon, issuer, tokens) for taxon in taxons])
    data = {"issuer": issuer, "average_price": sum(average_prices)/len(average_prices) if average_prices else 0, "floor_price": min(average_prices) if average_prices else 0}
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(data, f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}", "price.json")
    else:
        await AsyncS3FileWriter(
            Config.PRICE_DUMP_BUCKET
        ).write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data)


def invoke_issuer_pricing_dump(issuer):
    logger.info(f"Starting For Issuer: {issuer}")
    payload = {"issuer": issuer}
    lambda_client = boto3.client("lambda", region_name="eu-west-2", aws_access_key_id=Config.ACCESS_KEY_ID, aws_secret_access_key=Config.SECRET_ACCESS_KEY)
    resp = lambda_client.invoke(
        FunctionName='issuers-nft-pricing-dev',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )
    logger.info(resp)


def invoke_csv_dump():
    logger.info("Starting CSV Dump")
    lambda_client = boto3.client("lambda", region_name="eu-west-2", aws_access_key_id=Config.ACCESS_KEY_ID, aws_secret_access_key=Config.SECRET_ACCESS_KEY)
    resp = lambda_client.invoke(
        FunctionName='raw-csv-dump-dev',
        InvocationType='Event',
    )
    logger.info(resp)


def invoke_table_dump():
    logger.info("Invoking Table Dump")
    lambda_client = boto3.client("lambda", region_name="eu-west-2", aws_access_key_id=Config.ACCESS_KEY_ID,
                                 aws_secret_access_key=Config.SECRET_ACCESS_KEY)
    resp = lambda_client.invoke(
        FunctionName='nft-table-dump-dev',
        InvocationType='Event',
    )
    logger.info(resp)

async def xls20_raw_data_dump():
    issuers = factory.supported_issuers
    last_hour = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')
    # last_hour_2 = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d-%H')
    supply = await read_json(Config.NFT_DUMP_BUCKET, "supply.json", Config)
    supply_df = pd.DataFrame(supply)
    issuers_df = factory.issuers_df
    # logger.info(f"fetched issuers {issuers}")
    prices = await asyncio.gather(*[read_json(Config.PRICE_DUMP_BUCKET, f"{last_hour}/{issuer}/price.json", Config) for issuer in issuers])
    # logger.info("fetched prices")
    price_df = pd.DataFrame([price for price in prices if price is not None])
    supply_df.rename(columns={"issuer": "ISSUER", "supply": "SUPPLY", "circulation": "CIRCULATION"}, inplace=True)
    issuers_df.rename(columns={"Issuer_Account": "ISSUER", "Project_Name": "NAME", "Website_URL": "WEBSITE", "Twitter_URL": "TWITTER"}, inplace=True)
    price_df.rename(columns={"issuer": "ISSUER", "floor_price": "PRICEXRP"}, inplace=True)
    merged_1 = price_df.merge(supply_df, how="inner", on=["ISSUER"])
    final_merge = merged_1.merge(issuers_df, how="inner", on=["ISSUER"])
    final_df = final_merge[["ISSUER", "NAME", "WEBSITE", "TWITTER", "PRICEXRP", "SUPPLY", "CIRCULATION"]].copy()
    final_df["MARKET_CAP"] = final_df["SUPPLY"] * final_df["PRICEXRP"]
    final_df["HELD_0"] = final_df["SUPPLY"] - final_df["CIRCULATION"]
    final_df["HOLDER_COUNT"] = final_df["CIRCULATION"]
    final_df["TWITTER"] = final_df["TWITTER"].str.split("/").str[-1]
    await AsyncS3FileWriter(
        Config.RAW_DUMP_BUCKET
    ).write_df(final_df, f"{last_hour}.csv", "csv")
    invoke_table_dump()
    # await AsyncS3FileWriter(
    #     Config.RAW_DUMP_BUCKET
    # ).write_df(final_df, f"{last_hour_2}.csv", "csv")

async def table():
    current_time = datetime.datetime.utcnow()
    # day_ago = current_time - datetime.timedelta(days=1)
    current = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')
    previous = (datetime.datetime.strptime(current, '%Y-%m-%d-%H') - datetime.timedelta(days=1)).strftime('%Y-%m-%d-%H')
    df_previous = pd.read_csv(f"s3://{Config.RAW_DUMP_BUCKET}/{previous}.csv")
    df_previous.columns = [to_snake_case(col) for col in df_previous.columns]
    df = pd.read_csv(f"s3://{Config.RAW_DUMP_BUCKET}/{current}.csv")
    df.columns = [to_snake_case(x) for x in df.columns]
    df = df[df["twitter"].notna()]
    df["name"] = df["name"].str.strip()

    df["total_supply"] = df["supply"]
    df["circulating_supply"] = df["circulation"]

    df = pd.merge(df, df_previous, on="issuer", how="outer", suffixes=("", "_previous"))

    df["promoted"] = "false"

    df["id"] = df.index + 1

    df["logo_url"], df["banner_url"] = zip(*df["twitter"].apply(twitter_pics))

    df["project"] = df[["issuer", "logo_url", "banner_url", "name", "promoted"]].apply(
        lambda x: x.to_json(), axis=1
    )


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

    for name in twitter_list:
        for tweet in twitter_scrapper.TwitterSearchScraper("from:{}".format(name)).get_items():
            diff = (current_time - tweet.date.replace(tzinfo=None)).total_seconds()
            if diff < 0:
                continue
            if diff >= 3600 * 24 * 8:
                break
            if diff < 3600 * 24 * 7:
                tweets_list.append([name, 1, tweet.retweetCount, tweet.likeCount])
            if diff >= 3600 * 24 * 1 and diff < 3600 * 24 * 8:  # noqa
                tweets_list_previous.append([name, 1, tweet.retweetCount, tweet.likeCount])

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
    ).write_buffer("latest/NFT_Collections_Table.json", new_b)
