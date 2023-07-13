import asyncio
import os
import datetime

import pytest
from utilities import Config, Factory, execute_sql_file

async def test_db_setup(factory: Factory):
    db_client = factory.get_db_client()
    pool = await db_client.create_db_pool()
    async with pool.acquire() as connection:
        await execute_sql_file(connection, "sql/tests/test_db_setup.sql")
        # await execute_sql_file(connection, "sql/price_summary_setup.sql")


@pytest.fixture(scope="session")
def setup():
    config = Config.from_env(".env.test")
    factory = Factory(config)
    asyncio.run(test_db_setup(factory))
    yield {
        "config": config,
        "cwd": os.getcwd(),
        "last_hour": datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
    }
