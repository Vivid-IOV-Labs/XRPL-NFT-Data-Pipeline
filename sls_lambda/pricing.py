import asyncio
import time

import aiohttp
import datetime
import logging
import json

from billiard.pool import Pool


from .base import BaseLambdaRunner
from utilities import chunks

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

    async def _get_taxon_price_summary(self, issuers):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                issuers_str = ",".join("'{0}'".format(issuer) for issuer in issuers)
                query = f"SELECT issuer, taxon, MIN(floor_price) AS floor_price, MAX(max_buy_offer) AS max_buy_offer FROM nft_pricing_summary WHERE burn_offer_hash is NULL AND issuer IN ({issuers_str}) GROUP BY issuer, taxon"
                await cursor.execute(query)
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _get_volume(self, issuers):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                issuers_str = ",".join("'{0}'".format(issuer) for issuer in issuers)
                query = f"SELECT issuer, taxon, SUM(volume) as volume from nft_volume_summary WHERE burn_offer_hash is NULL AND issuer IN ({issuers_str}) GROUP BY issuer, taxon"
                await cursor.execute(query)
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _dump_issuer_taxon_volume(self, issuers):
        now = datetime.datetime.utcnow()
        volumes = await self._get_volume(issuers)
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
        issuers = self.factory.supported_issuers
        await self._dump_issuer_taxon_volume(issuers)
        prices = await self._get_taxon_price_summary(issuers)
        await asyncio.gather(*[self._dump_issuer_taxon_pricing(data) for data in prices])

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
                query = f"SELECT issuer, MIN(floor_price) AS floor_price, MAX(max_buy_offer) as max_buy_offer, (MAX(max_buy_offer)::DECIMAL + MIN(floor_price)::DECIMAL)/2 AS mid_price FROM nft_pricing_summary WHERE burn_offer_hash is NULL AND issuer IN ({issuers_str}) GROUP BY issuer;"
                await cursor.execute(query)
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

class OfferCurrencyPriceUpdate(BaseLambdaRunner):
    async def _fetch_offer_currencies(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT currency, issuer FROM nft_offer_currency;"
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _get_price(self, currency: str, issuer: str):  # noqa
        async with aiohttp.ClientSession() as session:  # noqa
            try:
                decoded_currency = bytes.fromhex(currency).decode('utf-8').replace("\x00", "")
            except ValueError:
                decoded_currency = currency
            url = f"https://api.onthedex.live/public/v1/ticker/{decoded_currency}.{issuer}:XRP"
            async with session.get(url) as response:  # noqa
                if response.status == 200:
                    content = await response.content.read()
                    to_dict = json.loads(content)
                    xrpl_quote = to_dict["pairs"]
                    if xrpl_quote:
                        return currency, xrpl_quote[0]["price_mid"]
                    return currency, 0
                else:
                    content = await response.content.read()
                    logger.error(f"Error Fetching Price: {content}")
                    return currency, 0

    async def _get_prices(self, currencies):
        print('getting prices')
        prices = []
        for chunk in chunks(currencies, 5):
            chunk_prices = await asyncio.gather(*[self._get_price(currency, issuer) for currency, issuer in chunk])
            prices.extend(chunk_prices)
            time.sleep(60)
        return prices

    async def _update_price(self, price):
        db_client = self.factory.get_db_client(write_proxy=True)
        currency, xrp_amount = price
        last_updated = datetime.datetime.utcnow().isoformat()
        pool = await db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"UPDATE nft_offer_currency SET last_updated = '{last_updated}', xrp_amount = {xrp_amount} WHERE currency = '{currency}';"
                )
            connection.close()

    async def _run(self):
        print('starting')
        currencies = await self._fetch_offer_currencies()
        print('fetched currencies')
        print(currencies)
        prices = await self._get_prices(currencies)
        print('price get complete')
        for price in prices:
            await self._update_price(price)

    def run(self) -> None:
        asyncio.run(self._run())
