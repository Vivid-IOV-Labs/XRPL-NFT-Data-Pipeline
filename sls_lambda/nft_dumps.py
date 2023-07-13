import asyncio
import datetime
import json
import time

import aiohttp

from utilities import chunks, Factory

from .base import BaseLambdaRunner


class NFTokenDump(BaseLambdaRunner):
    def __init__(self, factory: Factory):
        super().__init__(factory)
        self.base_url = "https://api.xrpldata.com/api/v1/xls20-nfts/issuer"
        self._set_writer("nft")

    async def _fetch_nfts(self, issuer):
        issuers_df = self.factory.issuers_df
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/{issuer}") as response:
                content = await response.content.read()
                data = json.loads(content)["data"]
                taxon = issuers_df[
                    issuers_df["Issuer_Account"] == issuer
                ].Taxon.to_list()
                if str(taxon[0]) != "nan":
                    target_nfts = [
                        nft for nft in data["nfts"] if nft["Taxon"] == taxon[0]
                    ]
                    data["nfts"] = target_nfts
                return data

    async def _dump_issuer_nfts(self, issuer: str):
        now = datetime.datetime.utcnow()
        data = await self._fetch_nfts(issuer)
        path = f"{now.strftime('%Y-%m-%d-%H')}/{issuer}.json"
        await self.writer.write_json(path, data)
        return_data = {
            "issuer": issuer,
            "supply": len(data["nfts"]),
            "circulation": len(
                [
                    token
                    for token in data["nfts"]
                    if token["Owner"].lower() != issuer.lower()
                ]
            ),
            "holders": len({token["Owner"] for token in data["nfts"]})
        }
        return_data["tokens_held"] = return_data["supply"] - return_data["circulation"]
        return return_data

    async def _run(self):
        supported_issuers = self.factory.supported_issuers
        supply = []
        for chunk in chunks(supported_issuers, 10):
            result = await asyncio.gather(
                *[self._dump_issuer_nfts(issuer) for issuer in chunk]
            )
            supply.extend(result)
            if self.factory.config.ENVIRONMENT == "TESTING":
                break
            time.sleep(60)
        await self.writer.write_json("supply.json", supply)

    def run(self):
        asyncio.run(self._run())



class NFTaxonDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self.base_url = "https://api.xrpldata.com/api/v1/xls20-nfts/taxon"
        self._set_writer("nft")

    async def _get_issuer_taxons(self, issuer):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/{issuer}") as response:
                content = await response.content.read()
                data = json.loads(content)
                return data["data"]

    async def _run(self):
        supported_issuers = self.factory.supported_issuers
        taxons = []
        for chunk in chunks(supported_issuers, 10):
            chunk_taxons = await asyncio.gather(
                *[self._get_issuer_taxons(issuer) for issuer in chunk]
            )
            taxons.extend(chunk_taxons)
            if self.factory.config.ENVIRONMENT == "TESTING":
                break
            time.sleep(60)
        await self.writer.write_json("taxon.json", taxons)

    def run(self) -> None:
        asyncio.run(self._run())
