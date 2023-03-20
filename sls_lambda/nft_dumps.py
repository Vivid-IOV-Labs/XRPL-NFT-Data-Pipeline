import asyncio
import datetime
import json
import time

import aiohttp

from .base import BaseLambdaRunner
from utilities import chunks


class NFTokenDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self.base_url = "https://api.xrpldata.com/api/v1/xls20-nfts/issuer"
        self._set_writer("nft")

    def _get_output_path(self, issuer: str) -> str:
        now = datetime.datetime.utcnow()
        if self.environment == "LOCAL":
            return f"{now.strftime('%Y-%m-%d-%H')}/{issuer}.json"
        return f"{now.strftime('%Y-%m-%d-%H')}/{issuer}.json"

    async def _fetch_nfts(self, issuer):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/{issuer}"
            ) as response:
                content = await response.content.read()
                data = json.loads(content)["data"]
                taxon = self.issuers_df[self.issuers_df["Issuer_Account"] == issuer].Taxon.to_list()
                if str(taxon[0]) != "nan":
                    target_nfts = [nft for nft in data["nfts"] if nft["Taxon"] == taxon[0]]
                    data["nfts"] = target_nfts
                return data

    async def _dump_issuer_nfts(self, issuer: str):
        data = await self._fetch_nfts(issuer)
        path = self._get_output_path(issuer)
        await self.writer.write_json(path, data)
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

    async def run(self):
        supply = []
        for chunk in chunks(self.supported_issuers, 10):
            result = await asyncio.gather(*[self._dump_issuer_nfts(issuer) for issuer in chunk])
            supply.extend(result)
            time.sleep(60)
        await self.writer.write_json("supply.json", supply)

class NFTaxonDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self.base_url = "https://api.xrpldata.com/api/v1/xls20-nfts/taxon"
        self._set_writer("nft")

    async def _get_issuer_taxons(self, issuer):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/{issuer}"
            ) as response:
                content = await response.content.read()
                data = json.loads(content)
                return data["data"]

    def _get_output_path(self) -> str:
        if self.environment == "LOCAL":
            return "taxon.json"
        return "taxon.json"

    async def run(self):
        taxons = []
        for chunk in chunks(self.supported_issuers, 10):
            chunk_taxons = await asyncio.gather(
                *[self._get_issuer_taxons(issuer) for issuer in chunk]
            )
            taxons.extend(chunk_taxons)
            time.sleep(60)
        path = self._get_output_path()
        await self.writer.write_json(path, taxons)
