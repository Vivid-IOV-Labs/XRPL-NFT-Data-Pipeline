import aioboto3
import aiopg


class DataBaseClient:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def _get_dsn(conn_info: dict):
        return "dbname={database} user={user} host={host} password={password}".format(
            **conn_info
        )

    async def get_proxy_token(self):
        session = aioboto3.Session(
            aws_access_key_id=self.config.ACCESS_KEY_ID,
            aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
        )
        async with session.client("rds") as rds:
            token = await rds.generate_db_auth_token(
                DBHostname=self.config.PROXY_HOST,
                Port=self.config.RDS_PORT,
                DBUsername=self.config.RDS_USER,
                Region="eu-west-2",
            )
            return token

    async def create_db_pool(self, max_size=1000):
        dsn = DataBaseClient._get_dsn(self.config.PROXY_CONN_INFO)
        pool = await aiopg.create_pool(dsn=dsn, maxsize=max_size)
        return pool