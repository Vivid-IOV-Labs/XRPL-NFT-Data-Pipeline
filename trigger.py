import asyncio
import sys

from utilities import execute_sql_file, factory


async def pricing_trigger_setup():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/price_summary_setup.sql")
        await execute_sql_file(connection, "sql/token_price_summary_update.sql")
        connection.close()

async def volume_trigger_setup():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/volume_summary_setup.sql")
        await execute_sql_file(connection, "sql/volume_summary_update.sql")
        connection.close()

async def xrp_amount_update():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/xrp_amount_update.sql")
        connection.close()

async def nft_burn_offer_trigger_setup():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/price_summary_update.sql")
        await execute_sql_file(connection, "sql/burn_offer_trigger.sql")
        connection.close()

async def pricing_summary_burn_offer_update():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/update_price_summary_burn_offer.sql")
        connection.close()

async def volume_summary_burn_offer_update():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO[
        "host"
    ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/volume_summary_table_update.sql")
        await execute_sql_file(connection, "sql/update_volume_summary_burn_offer_hash.sql")
        await execute_sql_file(connection, "sql/burn_offer_trigger.sql")
        connection.close()

if __name__ == "__main__":
    args = sys.argv
    if args[1] == "price-trigger":
        asyncio.run(pricing_trigger_setup())
    elif args[1] == "volume-trigger":
        asyncio.run(volume_trigger_setup())
    elif args[1] == "amount-update":
        asyncio.run(xrp_amount_update())
    elif args[1] == "burn-offer-trigger":
        asyncio.run(nft_burn_offer_trigger_setup())
    elif args[1] == "price-summary-burn-offer-update":
        asyncio.run(pricing_summary_burn_offer_update())
