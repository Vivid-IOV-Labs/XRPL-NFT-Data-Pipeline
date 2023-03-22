import asyncio
import datetime
from multiprocessing.pool import Pool
from .base import BaseLambdaRunner
import logging

from utilities import (chunks, fetch_dumped_token_prices, fetch_issuer_taxons,
                   fetch_issuer_tokens, read_json, fetch_dumped_taxon_prices)


logger = logging.getLogger("app_log")


class PricingLambdaRunner(BaseLambdaRunner):
    def _run(self, issuer):
        raise NotImplementedError
    def run(self) -> None:
        supported_issuers = self.factory.supported_issuers
        with Pool(10) as pool:
            pool.map(self._run, supported_issuers)


class NFTokenPriceDump(PricingLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("price")

    async def _get_token_offers(self, pool, token_id):  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT amount, is_sell_offer FROM nft_buy_sell_offers WHERE nft_token_id = '{token_id}'")  # noqa
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _get_taxon_offers(self, pool, taxon, issuer):  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT amount, is_sell_offer FROM nft_buy_sell_offers WHERE issuer = '{issuer}' AND taxon = '{taxon}'")  # noqa
                result = await cursor.fetchall()
            connection.close()
        return result

    # async def _dump_token_offer(self, issuer, token_id, pool):  # noqa
    #     """Fetches buy and sell offers for a particular token from the ledger"""
    #     now = datetime.datetime.utcnow()
    #     offers = await self._get_token_offers(pool, token_id)
    #     buy_offers = [float(offer[0]) for offer in offers if offer[1] is False]
    #     sell_offers = [float(offer[0]) for offer in offers if offer[1] is True]
    #     highest_buy_offer = max(buy_offers) if buy_offers else 0
    #     lowest_sell_offer = min(sell_offers) if sell_offers else 0
    #     average = highest_buy_offer + lowest_sell_offer / 2
    #     data = {
    #         "average": average,
    #         "lowest_sell": lowest_sell_offer,
    #     }
    #     await self.writer.write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/tokens/{token_id}.json", data)

    async def _dump_taxon_pricing(self, taxon, issuer, pool):
        now = datetime.datetime.utcnow()
        offers = await self._get_taxon_offers(pool, taxon, issuer)
        buy_offers = [float(offer[0]) for offer in offers if offer[1] is False and float(offer[0]) != 0]
        sell_offers = [float(offer[0]) for offer in offers if offer[1] is True and float(offer[0]) != 0]
        highest_buy_offer = max(buy_offers) if buy_offers else 0
        lowest_sell_offer = min(sell_offers) if sell_offers else 0
        average = highest_buy_offer + lowest_sell_offer / 2
        data = {
            "average": average,
            "lowest_sell": lowest_sell_offer,
        }
        await self.writer.write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/taxons/{taxon}.json", data)

    # async def _dump_taxon_offers(self, taxon, issuer, tokens):
    #     tokens = [token for token in tokens if token["Taxon"] == taxon]
    #     pool = await self.db_client.create_db_pool()
    #     for chunk in chunks(tokens, 500):
    #          await asyncio.gather(
    #              *[
    #                  self._dump_token_offer(issuer, token["NFTokenID"], pool)
    #                  for token in chunk
    #              ]
    #          )

    async def _dump_issuer_taxons_offer(self, issuer):  # noqa
        logger.info(f"Issuer --> {issuer}")
        issuers_df = self.factory.issuers_df
        taxon = issuers_df[issuers_df["Issuer_Account"] == issuer].Taxon.to_list()
        taxons = []
        if str(taxon[0]) != "nan":
            taxons = [int(taxon[0])]
        else:
            taxons = fetch_issuer_taxons(
                issuer,
                self.factory.config.ENVIRONMENT,
                self.factory.config.NFT_DUMP_BUCKET,
                self.factory.config.ACCESS_KEY_ID,
                self.factory.config.SECRET_ACCESS_KEY,
            )  # todo: refactor this
        # tokens = fetch_issuer_tokens(
        #     issuer,
        #     self.factory.config.ENVIRONMENT,
        #     self.factory.config.NFT_DUMP_BUCKET,
        #     self.factory.config.ACCESS_KEY_ID,
        #     self.factory.config.SECRET_ACCESS_KEY,
        # )  # todo: refactor this
        if len(taxons) > 100:
            for chunk in chunks(taxons, 100):
                pool = await self.db_client.create_db_pool()
                await asyncio.gather(*[self._dump_taxon_pricing(taxn, issuer, pool) for taxn in chunk])
                # await asyncio.gather(
                #     *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in chunk]
                # )
        else:
            pool = await self.db_client.create_db_pool()
            await asyncio.gather(*[self._dump_taxon_pricing(taxn, issuer, pool) for taxn in taxons])
            # await asyncio.gather(
            #     *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in taxons]
            # )

    def _run(self, issuer):
        asyncio.run(self._dump_issuer_taxons_offer(issuer))


class IssuerPriceDump(PricingLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("price")

    async def _dump_issuer_pricing(self, issuer):  # noqa
        """
        Loop through all the token prices and fetch the prices
        Floor price == lowest_sell_price over all taxons
        mid price == sum(average)/len(average)
        """
        now = datetime.datetime.utcnow()
        keys = fetch_dumped_taxon_prices(issuer, self.factory.config)
        prices = []
        for chunk in chunks(keys, 100):
            chunk_prices = await asyncio.gather(
                *[read_json(self.factory.config.PRICE_DUMP_BUCKET, key, self.factory.config) for key in chunk]  # todo: look into this
            )
            prices.extend(chunk_prices)
        sell_prices = [
            price["lowest_sell"] for price in prices if price["lowest_sell"] != 0
        ]  # noqa
        average_prices = [
            price["average"] for price in prices if price["average"] != 0
        ]  # noqa

        floor_price = min(sell_prices) if sell_prices else 0
        mid_price = sum(average_prices) / len(average_prices) if average_prices else 0
        data = {"issuer": issuer, "mid_price": mid_price, "floor_price": floor_price}
        await self.writer.write_json(f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data)

    def _run(self, issuer):
        asyncio.run(self._dump_issuer_pricing(issuer))
