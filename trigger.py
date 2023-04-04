import asyncio

from utilities import factory

async def execute_sql_file(conn, sql_file):
    with open(sql_file, 'r') as f:
        sql = f.read()
    async with conn.cursor() as cur:
        result = await cur.execute(sql)
        print(result)

async def pricing_trigger_setup():
    db_client = factory.get_db_client()
    db_client.config.PROXY_CONN_INFO["host"] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"

    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/price_summary_table.sql")
        await execute_sql_file(connection, "sql/price_summary_function.sql")
        await execute_sql_file(connection, "sql/price_summary_update_trigger.sql")
        connection.close()

asyncio.run(pricing_trigger_setup())