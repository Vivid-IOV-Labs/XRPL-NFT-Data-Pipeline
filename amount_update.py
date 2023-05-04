import asyncio
import aiohttp
import json

from utilities import factory


async def fetch_null_offers():
    db_client = factory.get_db_client()

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"SELECT amount_issuer, currency, amount, offer_index from nft_buy_sell_offers WHERE currency != '' AND xrp_amount is NULL"  # noqa
            )
            result = await cursor.fetchall()
            connection.close()
            return result

async def convert_to_xrp(currency, issuer):
    async with aiohttp.ClientSession() as session:
        if len(currency) > 4:
            currency = bytes.fromhex(currency).decode('utf-8').replace("\x00", "")
        url = f"https://api.onthedex.live/public/v1/ticker/{currency}.{issuer}:XRP"
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.content.read()
                to_dict = json.loads(content)
                price = to_dict['pairs'][0]['price_mid']
                return price


async def update_xrp_amount(offer_index, amount, currency, price_dict):
    total = float(amount) * float(price_dict[currency])
    to_droplets = total * 1000000
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-ro-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"UPDATE nft_buy_sell_offers SET xrp_amount = {to_droplets} WHERE offer_index = {offer_index}"
                # noqa
            )
            connection.close()
            print(f"Update Price For Offer Index: {offer_index} with Amount: {amount}")

async def run():
    null_offers = await fetch_null_offers()
    issuer_currencies = set([(data[0], data[1]) for data in null_offers])
    currency_prices = {}
    for pair in issuer_currencies:
        data = await convert_to_xrp(pair[1], pair[0])
        currency_prices[pair[1]] = data
    await asyncio.gather(*[update_xrp_amount(offer_index, amount, currency, currency_prices) for (issuer, currency, amount, offer_index) in null_offers])

if __name__ == "__main__":
    asyncio.run(run())
