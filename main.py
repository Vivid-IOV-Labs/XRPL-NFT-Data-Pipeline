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
file_handler = logging.FileHandler("logger.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
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
            now = datetime.datetime.now()
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
    tokens = [token for token in tokens if token["Taxon"] == taxon]
    logger.info(f"Running for Taxon {taxon} With {len(tokens)} Tokens")
    now = datetime.datetime.now()
    offers = []
    for chunk in chunks(tokens, 1000):
        averages = await asyncio.gather(*[get_taxon_token_offers(issuer, taxon, token["NFTokenID"]) for token in chunk])
        offers.extend([average for average in averages if average != 0])
    data = {"taxon": taxon, "issuer": issuer, "average_price": sum(offers)/len(offers) if offers else 0}
    if Config.ENVIRONMENT == "LOCAL":
        LocalFileWriter().write_json(data, f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}", f"{taxon}.json")
    else:
        await AsyncS3FileWriter(
            Config.PRICE_DUMP_BUCKET
        ).write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}-{taxon}.json", data)



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
    taxons = fetch_issuer_taxons(issuer, Config.ENVIRONMENT)
    tokens = fetch_issuer_tokens(issuer, Config.ENVIRONMENT)
    logger.info(f"Taxon Count: {len(taxons)}\nToken Count: {len(tokens)}")
    await asyncio.gather(*[taxon_offers(taxon, issuer, tokens) for taxon in taxons])




async def main():
    start = time.monotonic()
    await dump_issuers_nfts()
    await dump_issuer_taxon_offers("rDxThQhDkAaas4Mv22DWxUsZUZE1MfvRDf")
    print(f"Executed in {time.monotonic() - start}\n\n")
    # nft_sheet_df = pd.read_csv(Config.NFTS_SHEET_URL)
    # supported_issuers = nft_sheet_df["Issuer_Account"].values.tolist()
    # xls20_nfts = json.load(open("nfts.json", "r"))
    # target_nfts = [(issuer, xls20_nfts[issuer]) for issuer in xls20_nfts if issuer in supported_issuers]
    # start = time.monotonic()
    # for issuer, tokens in target_nfts[:5]:
    #     await issuer_pricing(issuer, tokens)
    #     logger.info(f"Completed Running For Issuer {issuer}. Going To Sleep For 60secs...\n\n")
    #     time.sleep(60)
    # print(f"Executed in {time.monotonic() - start}\n\n")

asyncio.run(main())
