import asyncio

from utilities import factory, chunks, LocalFileWriter
from typing import Dict
import aiohttp
import json
import time
import pandas as pd

issuers = factory.supported_issuers
issuer_taxon_map = {
    "rLtgE7FjDfyJy5FGY87zoAuKtH6Bfb9QnE":	16,
    "r3BWpaFk3rtWhhs2Q5FwFqLVdTPnfVUJLr":	9,
    "rUCjpVXSWM4tqnG49vHPn4adm7uoz5howG":	57,
    "r3a82jDJdg4TyUMEPEH4Wpg62HniXA4Jcj":	7,
    "r4zG9kcxyvq5niULmHbhUUbfh9R9nnNBJ4":	89939,
    "ra5jrnrq9BxsvzGeJY5XS9inftcJWMdJUx":	48,
    "rMsZProT3MjyCHP6FD9tk4A2WrwDMc6cbE":	0,
    "rKEGKWH2wKCyY2GNDtkNRqXhnYEsXZM2dP":	0,
    "rpbjkoncKiv1LkPWShzZksqYPzKXmUhTW7":	52,
    "rhsxg4xH8FtYc3eR53XDSjTGfKQsaAGaqm":	1,
    "rBLADEZyJhCtVS1vQm3UcvLac713KyGScN":	0,
    "rJ8vugKNvcRLrxxpHzuVC2HAa7W4BcA96f":	32,
    "rGGQVudQM1v1tqUESWZqrEbGDvLR8XKdiY":	1786,
    "rEzbi191M5AjrucxXKZWbR5QeyfpbedBcV":	1,
    "rKiNWUkVsq1rb9sWForfshDSEQDSUncwEu":	1,
    "rfUkZ3BVmgx5aD3Zo5bZk68hrUrhNth8y3":	1,
    "rG5qYqxdmDmLkVnPrLcWKE6LYTMeFGhYy9":	37,
    "rMp8iuddgiseUHE94KN7G9m6jruoNK8ht7":	0,
    "rDANq225BqjoyiFPXGcpBTzFdQTnn6aK6z":	10312,
    "rToXSFbQ8enso9A8zbmSrxhWkaNcU6yop":	1,
    "rDxThQhDkAaas4Mv22DWxUsZUZE1MfvRDf":	1089,
    "rUL4X4nLfarG9jEnsvpvoNgMcrYDaE2XHK":	10,
    "rKmSCJzc4pbQuAxyZDskozn2mkrNNppZJE":	14,
    "rwvQWhjpUncjEbhsD2V9tv4YpKXjfH5RDj":	1,
    "rHCRRCUEb2zJNV7FDrhzCivypExBcFT8Wy":	36,
    "rfkwiAyQx884xGGWUNDDhp5DjMTtwdrQVd":	39,
    "rBRFkq47qJpVN4JcL13dUQaLT1HfNuBctb":	50,
    "rUnbe8ZBmRQ7ef9EFnPd9WhXGY72GThCSc":	420,
    "r9ZW5tjbhKFLWxs4j1KqF61YSHAyDvo52D":	7,
    "rLoMBprALb22SkmNK4K3hFWrpgqXbAi6qQ":	20705
}
writer = LocalFileWriter()

async def fetch_project_offers_from_nixer_api(issuer, taxon):
    async with aiohttp.ClientSession() as session:  # noqa
        url = f"https://api.xrpldata.com/api/v1/xls20-nfts/offers/issuer/{issuer}/taxon/{taxon}"
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.content.read()
                to_dict = json.loads(content)
                return to_dict["data"]

async def fetch_offer_details_from_bithomp_api(offer_index: str):
    async with aiohttp.ClientSession(headers={"x-bithomp-token": "e2079870-7242-11ed-95dd-ab1ab9ffd825"}) as session:  # noqa
        url = f"https://bithomp.com/api/v2/nft-offer/{offer_index}"
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.content.read()
                to_dict = json.loads(content)
                return to_dict
            else:
                content = await response.content.read()
                print(f"Error Fetching Offer Details: {content}")

async def fetch_and_add_offer_details_to_map(offer_index: str, offer_map: Dict):
    offer_details = await fetch_offer_details_from_bithomp_api(offer_index)
    if offer_details is not None:
        offer_map[offer_index] = offer_details

async def dump_nixer_offer(issuer):
    taxon = issuer_taxon_map[issuer]
    offers = await fetch_project_offers_from_nixer_api(issuer, taxon)
    if offers:
        await writer.write_json(f"data/nixer-offers/{issuer}-{taxon}.json", offers)
    else:
        print(f"Error For Issuer: {issuer} & Taxon: {taxon}")

async def dump_nixer_offers_for_all_issuers():
    final_data = []
    for issuer in issuers:
        taxon = issuer_taxon_map[issuer]
        data = json.load(open(f"data/data/nixer-offers/{issuer}-{taxon}.json", "r"))
        for token in data["offers"]:
            for buy_offer in token["buy"]:
                to_append = {
                    "offer_id": buy_offer["OfferID"],
                    "token_id": buy_offer["NFTokenID"],
                    "issuer": issuer,
                    "taxon": taxon,
                    "amount_xrp": None,
                    "amount_value": None,
                    "amount_issuer": None,
                    "amount_currency": None,
                    "offer_type": "buy",
                    "offer_details": None
                }
                if type(buy_offer["Amount"]) != str:
                    amount_details = buy_offer["Amount"]
                    to_append["amount_value"] = amount_details["value"]
                    to_append["amount_issuer"] = amount_details["issuer"]
                    to_append["amount_currency"] = amount_details["currency"]
                else:
                    to_append["amount_xrp"] = buy_offer["Amount"]
                final_data.append(to_append)
            for sell_offer in token["sell"]:
                to_append = {
                    "offer_id": sell_offer["OfferID"],
                    "token_id": sell_offer["NFTokenID"],
                    "issuer": issuer,
                    "taxon": taxon,
                    "amount_xrp": None,
                    "amount_value": None,
                    "amount_issuer": None,
                    "amount_currency": None,
                    "offer_type": "sell",
                    "offer_details": None
                }
                if type(sell_offer["Amount"]) != str:
                    amount_details = sell_offer["Amount"]
                    to_append["amount_value"] = amount_details["value"]
                    to_append["amount_issuer"] = amount_details["issuer"]
                    to_append["amount_currency"] = amount_details["currency"]
                else:
                    to_append["amount_xrp"] = sell_offer["Amount"]
                final_data.append(to_append)
    await writer.write_json("nixer-offer-dump.json", final_data)


async def dump_all_offer_details():
    final_data = {}
    current_batch = 1
    all_offers = json.load(open("data/nixer-offer-dump.json", "r"))
    offer_ids = [offer["offer_id"] for offer in all_offers]
    try:
        dumped_offer_details = json.load(open("data/offer-map.json", "r"))
        final_data = dumped_offer_details
    except FileNotFoundError:
        dumped_offer_details = {}
    offers_to_fetch = list(set(offer_ids) - set(list(dumped_offer_details.keys())))
    print(f"Offers To Fetch: {len(offers_to_fetch)}\nNo of Batches: {len(offers_to_fetch)/100}")
    for chunk in chunks(offers_to_fetch, 100):
        await asyncio.gather(*[fetch_and_add_offer_details_to_map(offer_id, final_data) for offer_id in chunk])
        print(f"Completed Batch: {current_batch}")
        print(f"Offer Map Key Count: {len(final_data.keys())}")
        current_batch += 1
        await writer.write_json("offer-map.json", final_data)
        print("sleeping for 60 seconds...")
        time.sleep(60)

async def reformat_offer_details_dump():
    data = json.load(open("data/offer-map.json", "r"))
    offer_indexes = data.keys()
    new_data = [data[offer_index] for offer_index in offer_indexes]
    await writer.write_json("offer-map-formatted.json", new_data)

async def main():
    # await dump_nixer_offers_for_all_issuers()
    # await dump_all_offer_details()
    await reformat_offer_details_dump()

if __name__ == "__main__":
    start = time.monotonic_ns()
    asyncio.run(main())
    print(f"Executed in {(time.monotonic_ns() - start)} nanoseconds")