from main import factory
import datetime
from utils import read_json
import pandas as pd
import asyncio
from config import Config
from writers import AsyncS3FileWriter

async def xls20_csv_dump():
    issuers = factory.supported_issuers
    last_hour = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')
    supply = await read_json(Config.NFT_DUMP_BUCKET, "supply.json", Config)
    supply_df = pd.DataFrame(supply)
    issuers_df = factory.issuers_df
    prices = await asyncio.gather(*[read_json(Config.PRICE_DUMP_BUCKET, f"{last_hour}/{issuer}/price.json", Config) for issuer in issuers])
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
