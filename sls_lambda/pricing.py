import asyncio
import datetime
import logging

from billiard.pool import Pool

from utilities import (chunks, fetch_dumped_taxon_prices,
                       fetch_dumped_token_prices, fetch_issuer_taxons,
                       fetch_issuer_tokens, read_json, execute_sql_file)

from .base import BaseLambdaRunner

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
                    f"SELECT amount, is_sell_offer FROM nft_buy_sell_offers WHERE nft_token_id = '{token_id}'"  # noqa
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _get_taxon_offers(self, pool, taxon, issuer):  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT amount, is_sell_offer FROM nft_buy_sell_offers WHERE issuer = '{issuer}' AND taxon = '{taxon}'"  # noqa
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _dump_taxon_pricing(self, taxon, issuer, pool):
        now = datetime.datetime.utcnow()
        offers = await self._get_taxon_offers(pool, taxon, issuer)
        buy_offers = [
            float(offer[0])
            for offer in offers
            if offer[1] is False and float(offer[0]) != 0
        ]
        sell_offers = [
            float(offer[0])
            for offer in offers
            if offer[1] is True and float(offer[0]) != 0
        ]
        highest_buy_offer = max(buy_offers) if buy_offers else 0
        lowest_sell_offer = min(sell_offers) if sell_offers else 0
        average = highest_buy_offer + lowest_sell_offer / 2
        data = {
            "average": average,
            "lowest_sell": lowest_sell_offer,
        }
        await self.writer.write_json(
            f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/taxons/{taxon}.json", data
        )

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
                await asyncio.gather(
                    *[self._dump_taxon_pricing(taxn, issuer, pool) for taxn in chunk]
                )
                # await asyncio.gather(
                #     *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in chunk]
                # )
        else:
            pool = await self.db_client.create_db_pool()
            await asyncio.gather(
                *[self._dump_taxon_pricing(taxn, issuer, pool) for taxn in taxons]
            )
            # await asyncio.gather(
            #     *[self._dump_taxon_offers(taxon, issuer, tokens) for taxon in taxons]
            # )

    def _run(self, issuer):
        asyncio.run(self._dump_issuer_taxons_offer(issuer))


class TaxonPriceDump(PricingLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("taxon-price")

    async def _get_taxon_offers(self, pool, taxon, issuer):  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT amount, is_sell_offer FROM nft_buy_sell_offers WHERE issuer = '{issuer}' AND taxon = '{taxon}' AND currency = ''"  # noqa
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _get_taxon_price_summary(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT issuer, taxon, MIN(CASE WHEN is_sell_offer = TRUE AND amount::DECIMAL != 0 THEN amount END) AS floor_price, MAX(CASE WHEN is_sell_offer = FALSE THEN amount END) AS max_buy_offer FROM nft_buy_sell_offers WHERE currency = '' GROUP BY issuer, taxon" # noqa
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _dump_taxon_pricing(self, taxon, issuer, pool):
        now = datetime.datetime.utcnow()
        offers = await self._get_taxon_offers(pool, taxon, issuer)
        buy_offers = [
            float(offer[0])
            for offer in offers
            if offer[1] is False and float(offer[0]) != 0
        ]
        sell_offers = [
            float(offer[0])
            for offer in offers
            if offer[1] is True and float(offer[0]) != 0
        ]
        highest_buy_offer = max(buy_offers) if buy_offers else 0
        lowest_sell_offer = min(sell_offers) if sell_offers else 0
        mid_price = highest_buy_offer + lowest_sell_offer / 2
        data = {
            "mid_price": mid_price,
            "floor_price": lowest_sell_offer,
        }
        await self.writer.write_json(
            f"{issuer}/{taxon}/{now.strftime('%Y-%m-%d-%H')}.json", data
        )

    async def _dump_issuer_taxons_pricing(self, issuer):  # noqa
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
        if len(taxons) > 100:
            for chunk in chunks(taxons, 100):
                pool = await self.db_client.create_db_pool()
                await asyncio.gather(
                    *[self._dump_taxon_pricing(taxn, issuer, pool) for taxn in chunk]
                )
        else:
            pool = await self.db_client.create_db_pool()
            await asyncio.gather(
                *[self._dump_taxon_pricing(taxn, issuer, pool) for taxn in taxons]
            )

    async def _dump_issuer_taxon_pricing(self, data):
        now = datetime.datetime.now()
        issuer, taxon, floor_price, max_buy = data

        floor_price = float(floor_price) if floor_price is not None else 0
        max_buy = float(max_buy) if max_buy is not None else 0
        mid_price = (floor_price + max_buy) / 2
        new_data = {
            "floor_price": floor_price,
            "mid_price": mid_price
        }

        await self.writer.write_json(
            f"{issuer}/{taxon}/{now.strftime('%Y-%m-%d-%H')}.json",
            new_data
        )

    async def _dump_issuers_taxons_pricing(self):  # noqa
        prices = await self._get_taxon_price_summary()
        await asyncio.gather(*[self._dump_issuer_taxon_pricing(data) for data in prices])

    def _run(self, issuer):
        asyncio.run(self._dump_issuer_taxons_pricing(issuer))

    def run(self) -> None:
        asyncio.run(self._dump_issuers_taxons_pricing())

class IssuerPriceDump(PricingLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("price")

    async def _get_issuers_pricing(self, issuers):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                issuers_str = ",".join("'{0}'".format(issuer) for issuer in issuers)
                await cursor.execute(
                    f"SELECT issuer, MIN(floor_price) AS floor_price, MAX(max_buy_offer) as max_buy_offer, (MAX(max_buy_offer)::DECIMAL + MIN(floor_price)::DECIMAL)/2 AS mid_price FROM nft_pricing_summary WHERE issuer IN ({issuers_str}) GROUP BY issuer;"
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _dump_issuer_prices(self, pricing):
        now = datetime.datetime.utcnow()
        issuer = pricing[0]
        floor_price = float(pricing[1]) if pricing[1] is not None else 0
        max_buy_offer = float(pricing[2]) if pricing[2] is not None else 0
        mid_price = float(pricing[3]) if pricing[3] is not None else 0
        data = {
            "issuer": issuer,
            "floor_price": floor_price,
            "max_buy_offer": max_buy_offer,
            "mid_price": mid_price,
        }
        await self.writer.write_json(
            f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data
        )

    def _run(self, issuer):
        pass

    async def run(self) -> None:
        issuers = self.factory.supported_issuers
        prices = await self._get_issuers_pricing(issuers)
        await asyncio.gather(*[self._dump_issuer_prices(pricing) for pricing in prices])
