import asyncio
import aiohttp
import logging
import json
import httpx  # noqa
from config import Config
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models.requests import NFTBuyOffers, NFTSellOffers
from xrpl.models.response import ResponseStatus
from writers import LocalFileWriter, AsyncS3FileWriter
from factory import Factory
from utils import chunks, fetch_issuer_taxons, fetch_issuer_tokens
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
    offers.extend([float(offer["amount"]) for offer in buy_offers if type(offer["amount"]) != dict])
    offers.extend([float(offer["amount"]) for offer in sell_offers if type(offer["amount"]) != dict])
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
    tokens = [token for token in tokens if token["Taxon"] == taxon][:1000]  # temporary
    logger.info(f"Running for Taxon {taxon} With {len(tokens)} Tokens")
    now = datetime.datetime.utcnow()
    offers = []
    for chunk in chunks(tokens, 500):
        averages = await asyncio.gather(*[get_taxon_token_offers(issuer, taxon, token["NFTokenID"]) for token in chunk])
        offers.extend([average for average in averages if average != 0])
    data = {"taxon": taxon, "issuer": issuer, "average_price": sum(offers)/len(offers) if offers else 0}
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(data, f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}", f"{taxon}.json")
    else:
        await AsyncS3FileWriter(
            Config.PRICE_DUMP_BUCKET
        ).write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/{taxon}.json", data)
    return data["average_price"]



async def dump_issuers_nfts():
    supported_issuers = factory.supported_issuers
    for chunk in chunks(supported_issuers, 10):
        await asyncio.gather(*[dump_issuer_nfts(issuer) for issuer in chunk])
        time.sleep(60)


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
    taxons = taxons[:500]  # temporary
    average_prices = await asyncio.gather(*[taxon_offers(taxon, issuer, tokens) for taxon in taxons])
    data = {"issuer": issuer, "average_price": sum(average_prices)/len(average_prices) if average_prices else 0}
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(data, f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}", "price.json")
    else:
        await AsyncS3FileWriter(
            Config.PRICE_DUMP_BUCKET
        ).write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data)
