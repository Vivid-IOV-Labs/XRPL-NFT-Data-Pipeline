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

if __name__ == "__main__":
    args = sys.argv
    if args[1] == "price-trigger":
        asyncio.run(pricing_trigger_setup())
    elif args[1] == "volume-trigger":
        asyncio.run(volume_trigger_setup())
    elif args[1] == "amount-update":
        asyncio.run(xrp_amount_update())
