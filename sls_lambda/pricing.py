import asyncio
import datetime
from concurrent.futures import ThreadPoolExecutor
from .base import BaseLambdaRunner
import logging

from utilities import (chunks, fetch_dumped_token_prices, fetch_issuer_taxons,
                   fetch_issuer_tokens, read_json)


logger = logging.getLogger("app_log")

class NFTokenPriceDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)

    async def _get_token_offers(self, pool, token_id):  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT amount, is_sell_offer FROM nft_buy_sell_offers WHERE nft_token_id = '{token_id}'")  # noqa
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _dump_token_offer(self, issuer, token_id, pool):  # noqa
        """Fetches buy and sell offers for a particular token from the ledger"""
        now = datetime.datetime.utcnow()
        offers = await self._get_token_offers(pool, token_id)
        buy_offers = [float(offer[0]) for offer in offers if offer[1] is False]
        sell_offers = [float(offer[0]) for offer in offers if offer[1] is True]
        highest_buy_offer = max(buy_offers) if buy_offers else 0
        lowest_sell_offer = min(sell_offers) if sell_offers else 0
        average = highest_buy_offer + lowest_sell_offer / 2
        data = {
            "average": average,
            "lowest_sell": lowest_sell_offer,
        }
        self.writer.write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/tokens/{token_id}.json", data)

    async def _dump_taxon_offers(self, taxon, issuer, tokens):
        tokens = [token for token in tokens if token["Taxon"] == taxon]
        pool = await self.db_client.create_db_pool()
        for chunk in chunks(tokens, 100):
            await asyncio.gather(
                *[
                    self._dump_token_offer(issuer, token["NFTokenID"], pool)
                    for token in chunk
                ]
            )

    async def _dump_issuer_taxons_offer(self, issuer):  # noqa
        logger.info(f"Issuer --> {issuer}")
        issuers_df = self.issuers_df
        taxon = issuers_df[issuers_df["Issuer_Account"] == issuer].Taxon.to_list()
        taxons = []
        if str(taxon[0]) != "nan":
            taxons = [int(taxon[0])]
        else:
            taxons = fetch_issuer_taxons(
                issuer,
                self.environment,
                self.config.NFT_DUMP_BUCKET,
                self.config.ACCESS_KEY_ID,
                self.config.SECRET_ACCESS_KEY,
            )  # todo: refactor this
        tokens = fetch_issuer_tokens(
            issuer,
            self.environment,
            self.config.NFT_DUMP_BUCKET,
            self.config.ACCESS_KEY_ID,
            self.config.SECRET_ACCESS_KEY,
        )  # todo: refactor this
        if len(taxons) > 50:
            for chunk in chunks(taxons, 50):
                await asyncio.gather(
                    *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in chunk]
                )
        else:
            await asyncio.gather(
                *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in taxons]
            )

    def _run(self, issuer):
        asyncio.run(self._dump_issuer_taxons_offer(issuer))

    def run(self) -> None:
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(self._run, self.supported_issuers)
            for res in results:
                print(res)


class IssuerPriceDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)

    async def _dump_issuer_pricing(self, issuer):  # noqa
        """
        Loop through all the token prices and fetch the prices
        Floor price == lowest_sell_price over all taxons
        mid price == sum(average)/len(average)
        """
        now = datetime.datetime.utcnow()
        keys = fetch_dumped_token_prices(issuer, self.config)
        token_prices = []
        for chunk in chunks(keys, 100):
            chunk_prices = await asyncio.gather(
                *[read_json(self.config.PRICE_DUMP_BUCKET, key, self.config) for key in chunk]  # todo: look into this
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
        self.writer.write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data)

    def _run(self, issuer):
        asyncio.run(self._dump_issuer_pricing(issuer))

    def run(self) -> None:
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(self._run, self.supported_issuers)
            for res in results:
                print(res)
