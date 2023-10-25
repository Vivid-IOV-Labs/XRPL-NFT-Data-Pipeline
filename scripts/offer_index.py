import time
import json

import aiohttp
import pandas as pd
import asyncio

from utilities import LocalFileWriter, chunks, Config

INPUT_FILE = "data/xspectar.csv"
XRPL_SERVERS = ['https://xrplcluster.com/', 'https://s1.ripple.com:51234/', 'https://s2.ripple.com:51234/']
writer = LocalFileWriter()


async def fetch_offer_index(offer_hash: str, cfg: Config):
    async with aiohttp.ClientSession() as session:  # noqa
        payload = {
            "method": "tx",
            "params": [
                {
                    "transaction": offer_hash,
                    "binary": False
                }
            ]
        }
        for server in XRPL_SERVERS:
            try:
                async with session.post(server, data=payload) as response:  # noqa
                    if response.status == 200:
                        content = await response.content.read()
                        to_dict = json.loads(content)
                        data = dict()
                        data['hash'] = offer_hash
                        data['offer_index'] = to_dict['result']['meta']['offer_id']
                        data['token_id'] = to_dict['result']['NFTokenID']
                        return data
                    else:
                        content = await response.content.read()
                        print(f"Error Fetching Txn Details: {content}")
            except Exception as e:
                print(e)
                continue

async def dump(cfg: Config):
    offer_hashes = pd.read_csv(INPUT_FILE)["HASH"].to_list()
    final_data = []
    try:
        final_data = json.load(open("data/local/offer-index/details.json", "r"))
        fetched_index = [data['hash'] for data in final_data]
    except FileNotFoundError as e:
        fetched_index = []
        final_data = []
    to_fetch = list(set(offer_hashes) - set(fetched_index))
    for chunk in chunks(to_fetch, 5):
        details = await asyncio.gather(*[fetch_offer_index(address, cfg) for address in chunk])
        final_data.extend([detail for detail in details if detail is not None])
        fetched_index.extend(chunk)
        await writer.write_json("addresses/fetched.json", fetched_index)
        await writer.write_json("addresses/details.json", final_data)
        print("sleeping for 60s ...")
        time.sleep(60)
