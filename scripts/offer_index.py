import json
import time

import aiohttp
import pandas as pd
import asyncio

from utilities import LocalFileWriter, chunks

INPUT_FILE = "data/hashes.csv"
XRPL_SERVERS = ['https://xrplcluster.com/', 'https://s1.ripple.com:51234/', 'https://s2.ripple.com:51234/']
writer = LocalFileWriter()


async def fetch_offer_index(offer_hash: str):
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
                async with session.post(server, data=json.dumps(payload)) as response:  # noqa
                    if response.status == 200:
                        content = await response.content.read()
                        to_dict = json.loads(content)
                        data = dict()
                        data['hash'] = offer_hash
                        data['offer_index'] = to_dict['result']['meta'].get('offer_id', None)
                        data['token_id'] = to_dict['result']['NFTokenID']
                        return data
                    else:
                        #print(f'Error with status code: {response.status}')
                        continue
            except Exception as e:
                print(f'Error: {e}')
                continue

async def dump():
    offer_hashes = pd.read_csv(INPUT_FILE)["HASH"].to_list()
    final_data = []
    try:
        final_data = json.load(open("data/local/offer-index/offer-index-details-2.json", "r"))
        fetched_index = [data['hash'] for data in final_data]
    except FileNotFoundError:
        fetched_index = []
        final_data = []
    to_fetch = list(set(offer_hashes) - set(fetched_index))
    for chunk in chunks(to_fetch, 50):
        details = await asyncio.gather(*[fetch_offer_index(address) for address in chunk])
        final_data.extend([detail for detail in details if detail is not None])
        fetched_index.extend(chunk)
        await writer.write_json("offer-index/fetched-2.json", fetched_index)
        await writer.write_json("offer-index/offer-index-details-2.json", final_data)
        print("sleeping for 60s ...")
        time.sleep(60)
