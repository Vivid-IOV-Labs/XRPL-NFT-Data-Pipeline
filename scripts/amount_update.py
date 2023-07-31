import asyncio
import aiohttp
import json

from utilities import Factory, chunks, DataBaseClient, Config


async def fetch_null_offers(db_client: DataBaseClient):
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


async def update_xrp_amount(offer_index, amount, currency, price_dict, db_client: DataBaseClient):
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

async def fetch_nft_projects(db_client: DataBaseClient):
    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"SELECT nft_token_id, issuer, uri, taxon FROM project_tracker WHERE taxon is not NULL"  # noqa
            )
            result = await cursor.fetchall()
            connection.close()
            return result

async def run():
    # Create the Config and Factory Classes
    config = Config.from_env('.env')
    factory = Factory(config)

    # Fetch offers from the DB where xrpl_amount is NULL
    null_offers = await fetch_null_offers(factory.get_db_client(write_proxy=True))

    # Fetch the XRPL Value of each unique currency in the null offers
    issuer_currencies = set([(data[0], data[1]) for data in null_offers])
    currency_prices = {}
    for pair in issuer_currencies:
        data = await convert_to_xrp(pair[1], pair[0])
        currency_prices[pair[1]] = data

    # Update The XRPL AMOUNT column in the database
    for chunk in chunks(null_offers, 100):
        await asyncio.gather(*[update_xrp_amount(offer_index, amount, currency, currency_prices, factory.get_db_client(write_proxy=True)) for (issuer, currency, amount, offer_index) in chunk])

if __name__ == "__main__":
    asyncio.run(run())
