import aiopg
from .config import Config


class DataBaseClient:
    def __init__(self, config: Config):
        self.config = config

    def _get_dsn(self):
        conn_info = self.config.DB_CONN_INFO
        return "dbname={database} user={user} host={host} password={password}".format(
            **conn_info
        )


    async def create_db_pool(self, max_size=10):
        dsn = self._get_dsn()
        pool = await aiopg.create_pool(dsn=dsn, maxsize=max_size, timeout=1000)
        return pool
