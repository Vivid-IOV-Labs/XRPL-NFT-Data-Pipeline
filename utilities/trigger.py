from .factory import Factory
from .utils import execute_sql_file
from typing import Optional
from datetime import datetime

class TriggerManager:
    def __init__(
            self,
            factory: Factory,
            trigger_create_script: str,
            function_script: str,
            table_create_script: Optional[str] = None,
            allow_table_create: bool = False
    ):
        self.db_client = factory.get_db_client(write_proxy=True)
        self.trigger_create_script = trigger_create_script
        self.function_script = function_script
        self.table_create_script = table_create_script
        self.allow_table_create = allow_table_create

    async def create_trigger(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            if self.allow_table_create:
                await execute_sql_file(connection, self.table_create_script)
            await execute_sql_file(connection, self.function_script)
            await execute_sql_file(connection, self.trigger_create_script)
            datetime.now().strftime("%Y-%m-%d:%H-%M")
            print(f"Trigger Creation Time: {datetime.utcnow().strftime('%Y/%m/%d-%H:%M')}")
            print(f"Trigger Creation Timestamp: {datetime.utcnow().timestamp()}")
            connection.close()

    async def update_function(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            await execute_sql_file(connection, self.function_script)
            connection.close()