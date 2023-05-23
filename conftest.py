import asyncio

import pytest
from utilities import Config, Factory, execute_sql_file

async def test_db_setup(factory: Factory):
    db_client = factory.get_db_client()
    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/test_db_setup.sql")
        await execute_sql_file(connection, "sql/price_summary_setup.sql")


@pytest.fixture(scope="session")
def setup():
    Config.ENVIRONMENT = "LOCAL"
    Config.DB_BASE_CONN_INFO = {
        "port": "5432",
        "database": "pkt_test",
        "user": "postgres",
        "password": "postgres",
    }
    Config.PROXY_CONN_INFO = {"host": "localhost", **Config.DB_BASE_CONN_INFO}
    factory = Factory(Config)
    asyncio.run(test_db_setup(factory))
    yield {"factory": factory}
