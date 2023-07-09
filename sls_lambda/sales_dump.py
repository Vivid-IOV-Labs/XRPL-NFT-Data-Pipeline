import asyncio

from .base import BaseLambdaRunner
import datetime

class NFTSalesDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("data")

    async def _get_sales_from_db(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT SUM(sales) FROM nft_sales;"
                )
                result = await cursor.fetchall()
            connection.close()
        return result
    async def _run(self) -> None:
        last_hour = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
        sales = await self._get_sales_from_db()
        sales = int(sales[0][0])
        data = {"hour": last_hour, "sales": sales}
        await self.writer.write_json(f"sales/{last_hour}.json", data)

    def run(self) -> None:
        asyncio.run(self._run())
