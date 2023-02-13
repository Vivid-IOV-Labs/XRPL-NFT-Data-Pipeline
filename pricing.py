import asyncio
import datetime

import httpx  # noqa
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.constants import XRPLException
from xrpl.models.requests import NFTBuyOffers, NFTSellOffers
from xrpl.models.response import ResponseStatus

from config import Config
from invokers import invoke_issuer_price_dump, invoke_token_pricing_dump
from main import factory, logger
from utils import (chunks, fetch_dumped_token_prices, fetch_issuer_taxons,
                   fetch_issuer_tokens, read_json)
from writers import AsyncS3FileWriter


class Pricing:
    async def _dump_token_offer(self, issuer, token_id, server):  # noqa
        """Fetches buy and sell offers for a particular token from the ledger"""
        now = datetime.datetime.utcnow()
        client = AsyncJsonRpcClient(server)
        buy_offer_request = NFTBuyOffers(nft_id=token_id)
        sell_offer_request = NFTSellOffers(nft_id=token_id)
        buy_offers = await client.request(buy_offer_request)
        sell_offers = await client.request(sell_offer_request)

        # offers = []
        buy_offers = (
            buy_offers.result["offers"]
            if buy_offers.status == ResponseStatus.SUCCESS
            else []
        )
        sell_offers = (
            sell_offers.result["offers"]
            if sell_offers.status == ResponseStatus.SUCCESS
            else []
        )
        buy_offers_amount_arr = [
            float(offer["amount"])
            for offer in buy_offers
            if type(offer["amount"]) != dict
        ]
        sell_offers_amount_arr = [
            float(offer["amount"])
            for offer in sell_offers
            if type(offer["amount"]) != dict
        ]
        highest_buy_offer = max(buy_offers_amount_arr) if buy_offers_amount_arr else 0
        lowest_sell_offer = min(sell_offers_amount_arr) if sell_offers_amount_arr else 0
        average = highest_buy_offer + lowest_sell_offer / 2
        data = {
            "average": average,
            "lowest_sell": lowest_sell_offer,
        }
        await AsyncS3FileWriter(Config.PRICE_DUMP_BUCKET).write_json(
            f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/tokens/{token_id}.json", data
        )

    async def _dump_taxon_token_offers(self, issuer, taxon, token_id):
        for server in Config.XRPL_SERVERS:
            try:
                await self._dump_token_offer(issuer, token_id, server)
                return
            except (httpx.RequestError, XRPLException) as e:
                logger.error(f"Failed Using {server} with Error {e}: Retrying ...")
                continue
        logger.error(
            f"Could not get offers from any server for issuer {issuer}, taxon {taxon}, and token {token_id}"
        )

    async def _dump_taxon_offers(self, taxon, issuer, tokens):
        tokens = [token for token in tokens if token["Taxon"] == taxon]
        for chunk in chunks(tokens, 50):
            await asyncio.gather(
                *[
                    self._dump_taxon_token_offers(issuer, taxon, token["NFTokenID"])
                    for token in chunk
                ]
            )

    async def dump_issuer_taxons_offer(self, issuer):  # noqa
        logger.info(f"Issuer --> {issuer}")
        issuers_df = factory.issuers_df
        taxon = issuers_df[issuers_df["Issuer_Account"] == issuer].Taxon.to_list()
        taxons = []
        if str(taxon[0]) != "nan":
            taxons = [int(taxon[0])]
        else:
            taxons = fetch_issuer_taxons(
                issuer,
                Config.ENVIRONMENT,
                Config.NFT_DUMP_BUCKET,
                Config.ACCESS_KEY_ID,
                Config.SECRET_ACCESS_KEY,
            )
        tokens = fetch_issuer_tokens(
            issuer,
            Config.ENVIRONMENT,
            Config.NFT_DUMP_BUCKET,
            Config.ACCESS_KEY_ID,
            Config.SECRET_ACCESS_KEY,
        )
        if len(taxons) > 50:
            for chunk in chunks(taxons, 50):
                await asyncio.gather(
                    *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in chunk]
                )
        else:
            await asyncio.gather(
                *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in taxons]
            )

    async def dump_issuer_pricing(self, issuer):  # noqa
        """
        Loop through all the token prices and fetch the prices
        Floor price == lowest_sell_price over all taxons
        mid price == sum(average)/len(average)
        """
        now = datetime.datetime.utcnow()
        keys = fetch_dumped_token_prices(issuer, Config)
        token_prices = []
        for chunk in chunks(keys, 100):
            chunk_prices = await asyncio.gather(
                *[read_json(Config.PRICE_DUMP_BUCKET, key, Config) for key in chunk]
            )
            token_prices.extend(chunk_prices)
            logger.info(len(token_prices))
        sell_prices = [
            price["lowest_sell"] for price in token_prices if price["lowest_sell"] != 0
        ]  # noqa
        average_prices = [
            price["average"] for price in token_prices if price["average"] != 0
        ]  # noqa

        floor_price = min(sell_prices) if sell_prices else 0
        mid_price = sum(average_prices) / len(average_prices)
        data = {"issuer": issuer, "mid_price": mid_price, "floor_price": floor_price}
        await AsyncS3FileWriter(Config.PRICE_DUMP_BUCKET).write_json(
            f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data
        )

    async def dump_issuers_taxons_offer(self):  # noqa
        issuers = factory.supported_issuers
        await asyncio.gather(*[invoke_token_pricing_dump(issuer) for issuer in issuers])

    async def dump_issuers_pricing(self):  # noqa
        issuers = factory.supported_issuers
        await asyncio.gather(*[invoke_issuer_price_dump(issuer) for issuer in issuers])
