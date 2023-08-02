import asyncio
import time

import aiohttp
import pandas as pd
import logging
import json
from typing import Dict
from utilities import LocalFileWriter, chunks

class CreateOfferDump:
    def __init__(self, logger: logging.Logger):
        self.RPC_URL_1 = "https://s1.ripple.com:51234/"
        self.RPC_URL_2 = "https://s2.ripple.com:51234/"
        self.writer = LocalFileWriter()
        self.logger = logger

    async def _dump_tx_details(self, tx_hash: str, dumped: Dict):
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
            async with session.post(self.RPC_URL_2, data=json.dumps(payload)) as response:
                if response.status == 200:
                    content = await response.content.read()
                    data = json.loads(content)["result"]
                    await self.writer.write_json(f"tx/details/{tx_hash}.json", data)
                    dumped[tx_hash] = True
                    return data
                else:
                    content = await response.content.read()
                    data = json.loads(content)
                    self.logger.error(f"Error Fetching Tx: {tx_hash} --> {data}")
                    dumped[tx_hash] = False

    def _combined_dumps(self):
        df = pd.read_csv("data/xpunks_results.tsv", sep="\t")
        hashes = df["HASH"].to_list()
        combined = []
        for tx_hash in hashes:
            data = json.load(open(f"data/local/tx/details/{tx_hash}.json", "r"))
            combined.append(data)
        asyncio.run(self.writer.write_json("tx/tx-combined.json", combined))

    async def _run(self):
        df = pd.read_csv("data/xpunks_results.tsv", sep="\t")
        hashes = df["HASH"].to_list()
        dumped = json.load(open("data/local/tx/dumped.json", "r"))
        remainder = list(set(hashes) - set(list(dumped.keys())))
        self.logger.info(f"Remainder Count: {len(remainder)}")
        current_batch = 1
        for chunk in chunks(remainder, 1000):
            self.logger.info(f"started batch: {current_batch}")
            try:
                await asyncio.gather(*[self._dump_tx_details(tx_hash, dumped) for tx_hash in chunk])
                await self.writer.write_json("tx/dumped.json", dumped)
            except Exception as e:
                self.logger.error(e)
            self.logger.info(f"Done with Batch: {current_batch}\nSleeping for 60secs...\n")
            current_batch += 1
            time.sleep(60)

    def run(self):
        asyncio.run(self._run())
        self._combined_dumps()
