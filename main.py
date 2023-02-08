import asyncio
import aiohttp
import logging
import json

import boto3
import httpx  # noqa
from config import Config


from writers import LocalFileWriter, AsyncS3FileWriter
from factory import Factory
from utils import chunks, fetch_issuer_taxons, fetch_issuer_tokens, read_json
import pandas as pd  # noqa
import time
import datetime


logger = logging.getLogger("app_log")
formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)
factory = Factory()


async def dump_issuer_nfts(issuer):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://api.xrpldata.com/api/v1/xls20-nfts/issuer/{issuer}') as response:
            content = await response.content.read()
            data = json.loads(content)["data"]
            issuers_df = factory.issuers_df
            taxon = issuers_df[issuers_df["Issuer_Account"] == issuer].Taxon.to_list()
            if str(taxon[0]) != "nan":
                target_nfts = [nft for nft in data["nfts"] if nft["Taxon"] == taxon[0]]
                data["nfts"] = target_nfts
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
                "circulation": len([token for token in data["nfts"] if token["Owner"].lower() != issuer.lower()])
            }


async def get_issuer_taxons(issuer):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://api.xrpldata.com/api/v1/xls20-nfts/taxon/{issuer}') as response:
            content = await response.content.read()
            data = json.loads(content)
            return data["data"]


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


def invoke_graph_dump():
    logger.info("Invoking Graph Dump")
    lambda_client = boto3.client("lambda", region_name="eu-west-2", aws_access_key_id=Config.ACCESS_KEY_ID,
                                 aws_secret_access_key=Config.SECRET_ACCESS_KEY)
    resp = lambda_client.invoke(
        FunctionName='nft-graph-dump-dev',
        InvocationType='Event',
    )
    logger.info(resp)


def invoke_twitter_dump():
    logger.info("Invoking Twitter Dump")
    lambda_client = boto3.client("lambda", region_name="eu-west-2", aws_access_key_id=Config.ACCESS_KEY_ID,
                                 aws_secret_access_key=Config.SECRET_ACCESS_KEY)
    resp = lambda_client.invoke(
        FunctionName='nft-twitter-dump-dev',
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
    await AsyncS3FileWriter(
        Config.RAW_DUMP_BUCKET
    ).write_df(price_df, f"{last_hour}_price.csv", "csv")
    invoke_table_dump()
    invoke_graph_dump()
    invoke_twitter_dump()
    # await AsyncS3FileWriter(
    #     Config.RAW_DUMP_BUCKET
    # ).write_df(final_df, f"{last_hour_2}.csv", "csv")

