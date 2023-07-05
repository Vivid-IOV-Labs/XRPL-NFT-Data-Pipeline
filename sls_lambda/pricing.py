import asyncio
import datetime
import logging

from billiard.pool import Pool


from .base import BaseLambdaRunner

logger = logging.getLogger("app_log")


class PricingLambdaRunner(BaseLambdaRunner):
    def _run(self, issuer):
        raise NotImplementedError

    def run(self) -> None:
        supported_issuers = self.factory.supported_issuers
        with Pool(10) as pool:
            pool.map(self._run, supported_issuers)


class TaxonPriceDump(PricingLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("taxon-price")

    async def _get_taxon_price_summary(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT issuer, taxon, MIN(floor_price) AS floor_price, MAX(max_buy_offer) AS max_buy_offer FROM nft_pricing_summary WHERE burn_offer_hash is NULL GROUP BY issuer, taxon" # noqa
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _get_volume(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT issuer, taxon, SUM(volume) as volume from nft_volume_summary WHERE burn_offer_hash is NULL GROUP BY issuer, taxon"  # noqa
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _dump_issuer_taxon_volume(self):
        now = datetime.datetime.utcnow()
        volumes = await self._get_volume()
        data = [{"issuer": issuer, "taxon": taxon, "volume": int(volume) if volume is not None else 0 } for (issuer, taxon, volume) in volumes]
        await self.writer.write_json(f"volume/{now.strftime('%Y-%m-%d-%H')}.json", data)


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
        await self._dump_issuer_taxon_volume()
        prices = await self._get_taxon_price_summary()
        await asyncio.gather(*[self._dump_issuer_taxon_pricing(data) for data in prices])

    def _run(self, issuer):
        pass

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
                    f"SELECT issuer, MIN(floor_price) AS floor_price, MAX(max_buy_offer) as max_buy_offer, (MAX(max_buy_offer)::DECIMAL + MIN(floor_price)::DECIMAL)/2 AS mid_price FROM nft_pricing_summary WHERE burn_offer_hash is NULL AND issuer IN ({issuers_str}) GROUP BY issuer;"
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
