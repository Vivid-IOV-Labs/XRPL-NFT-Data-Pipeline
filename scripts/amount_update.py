import asyncio
import aiohttp
import json

from utilities import Factory, chunks

class XRPAmountUpdate:
    def __init__(self, factory: Factory):
        self.factory = factory
        self.base_url = "https://api.onthedex.live/public/v1"

    async def _fetch_null_offers(self):
        pool = await self.factory.get_db_client().create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT amount_issuer, currency, amount, offer_index from nft_buy_sell_offers WHERE currency != '' AND xrp_amount is NULL"  # noqa
                )
                result = await cursor.fetchall()
                connection.close()
                return result

    async def _convert_to_xrp(self, currency: str, issuer: str):
        async with aiohttp.ClientSession() as session:
            if len(currency) > 4:
                currency = bytes.fromhex(currency).decode('utf-8').replace("\x00", "")
            url = f"{self.base_url}/ticker/{currency}.{issuer}:XRP"
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.content.read()
                    to_dict = json.loads(content)
                    price = to_dict['pairs'][0]['price_mid']
                    return price

    async def _update_xrp_amount(self, offer_index, amount, currency, price_dict):
        db_client = self.factory.get_db_client(write_proxy=True)
        total = float(amount) * float(price_dict[currency])
        to_droplets = total * 1000000

        pool = await db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    await cursor.execute(
                        f"UPDATE nft_buy_sell_offers SET xrp_amount = {to_droplets} WHERE offer_index = '{offer_index}'"
                        # noqa
                    )
                except Exception as e:
                    print(e)
                    return
                connection.close()
                print(f"Update Price For Offer Index: {offer_index} with Amount: {amount} And XRP: {to_droplets}")

    async def fetch_nft_projects(self):
        db_client = self.factory.get_db_client()
        pool = await db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT nft_token_id, issuer, uri, taxon FROM project_tracker WHERE taxon is not NULL"  # noqa
                )
                result = await cursor.fetchall()
                connection.close()
                return result

    async def _run(self):
        # Fetch offers from the DB where xrpl_amount is NULL
        null_offers = await self._fetch_null_offers()

        # Fetch the XRPL Value of each unique currency in the null offers
        issuer_currencies = set([(data[0], data[1]) for data in null_offers])
        currency_prices = {}
        for pair in issuer_currencies:
            data = await self._convert_to_xrp(pair[1], pair[0])
            currency_prices[pair[1]] = data

        # Update The XRPL AMOUNT column in the database
        for chunk in chunks(null_offers, 100):
            await asyncio.gather(*[self._update_xrp_amount(offer_index, amount, currency, currency_prices) for (issuer, currency, amount, offer_index) in chunk])

    def run(self):
        asyncio.run(self._run())
