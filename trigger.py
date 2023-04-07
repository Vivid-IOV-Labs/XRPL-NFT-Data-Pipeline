import asyncio

from utilities import factory, execute_sql_file

async def pricing_trigger_setup():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO["host"] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/price_summary_setup.sql")
        await execute_sql_file(connection, "sql/token_price_summary_update.sql")
        connection.close()

asyncio.run(pricing_trigger_setup())