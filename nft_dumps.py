import asyncio
import datetime
import json
import time

import aiohttp

from config import Config
from main import factory
from utils import chunks
from writers import AsyncS3FileWriter, LocalFileWriter


async def dump_issuer_nfts(issuer):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://api.xrpldata.com/api/v1/xls20-nfts/issuer/{issuer}"
        ) as response:
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
                    data, f"data/nfts/{now.strftime('%Y-%m-%d-%H')}", f"{issuer}.json"
                )
            else:
                await AsyncS3FileWriter(Config.NFT_DUMP_BUCKET).write_json(
                    f"{now.strftime('%Y-%m-%d-%H')}/{issuer}.json", data
                )
            return {
                "issuer": issuer,
                "supply": len(data["nfts"]),
                "circulation": len(
                    [
                        token
                        for token in data["nfts"]
                        if token["Owner"].lower() != issuer.lower()
                    ]
                ),
            }


async def get_issuer_taxons(issuer):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://api.xrpldata.com/api/v1/xls20-nfts/taxon/{issuer}"
        ) as response:
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
    await AsyncS3FileWriter(Config.NFT_DUMP_BUCKET).write_json("supply.json", supply)


async def dump_issuers_taxons():
    supported_issuers = factory.supported_issuers
    taxons = []
    for chunk in chunks(supported_issuers, 10):
        chunk_taxons = await asyncio.gather(
            *[get_issuer_taxons(issuer) for issuer in chunk]
        )
        taxons.extend(chunk_taxons)
        time.sleep(60)
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(taxons, "data/nfts", "taxon.json")
    else:
        await AsyncS3FileWriter(Config.NFT_DUMP_BUCKET).write_json("taxon.json", taxons)
