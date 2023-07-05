import asyncio
import time

import aiohttp
import pandas as pd
import json
from typing import Dict
from utilities import LocalFileWriter, chunks

JSON_RPC_URL_1 = "https://s1.ripple.com:51234/"
JSON_RPC_URL_2 = "https://s2.ripple.com:51234/"
writer = LocalFileWriter()


async def dump_tx_details(tx_hash: str, dumped: Dict):
    if dumped.get(tx_hash, False):
        print(f"Already Dumped Tx: {tx_hash}")
        return
    payload = {
        "method": "tx",
        "params": [
            {
                "transaction": tx_hash,
                "binary": False
            }
        ]
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(JSON_RPC_URL_2, data=json.dumps(payload)) as response:
            if response.status == 200:
                content = await response.content.read()
                data = json.loads(content)["result"]
                await writer.write_json(f"tx/details/{tx_hash}.json", data)
                dumped[tx_hash] = True
                return data
            else:
                content = await response.content.read()
                data = json.loads(content)
                print(f"Error Fetching Tx: {tx_hash} --> {data}")
                dumped[tx_hash] = False

def combined_dumps():
    df = pd.read_csv("data/xpunks_results.tsv", sep="\t")
    hashes = df["HASH"].to_list()
    combined = []
    for tx_hash in hashes:
        data = json.load(open(f"data/local/tx/details/{tx_hash}.json", "r"))
        combined.append(data)
    asyncio.run(writer.write_json("tx/tx-combined.json", combined))

async def main():
    df = pd.read_csv("data/xpunks_results.tsv", sep="\t")
    hashes = df["HASH"].to_list()
    dumped = json.load(open("data/local/tx/dumped.json", "r"))
    remainder = list(set(hashes) - set(list(dumped.keys())))
    print(f"Remainder Count: {len(remainder)}")
    current_batch = 1
    for chunk in chunks(remainder, 1000):
        print(f"started batch: {current_batch}")
        try:
            await asyncio.gather(*[dump_tx_details(tx_hash, dumped) for tx_hash in chunk])
            await writer.write_json("tx/dumped.json", dumped)
        except Exception as e:
            print(e)
        print(f"Done with Batch: {current_batch}\nSleeping for 60secs...\n")
        current_batch += 1
        time.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
    combined_dumps()
