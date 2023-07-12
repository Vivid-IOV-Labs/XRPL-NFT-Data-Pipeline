import os
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class Config:
    ENVIRONMENT: str
    ACCESS_KEY_ID: str
    SECRET_ACCESS_KEY: str
    PRICE_DUMP_BUCKET: str
    TAXON_PRICE_DUMP_BUCKET: str
    NFT_DUMP_BUCKET: str
    NFTS_SHEET_URL: str
    RAW_DUMP_BUCKET: str
    DATA_DUMP_BUCKET: str
    TWITTER_API_KEY: str
    TWITTER_API_SECRET: str
    STAGE: str
    PROXY_CONN_INFO: Dict
    DB_CONN_INFO: Dict
    WRITE_PROXY: str

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> 'Config':
        """
        Creates An ENV object from a .env file
        :param env_file: Path to the .env file
        :return: BaseConfig
        """
        if env_file is not None:
            from dotenv import load_dotenv
            load_dotenv(env_file, override=True)
        base_db_conn = {
            "port": os.environ["DB_PORT"],
            "database": os.environ["DB_NAME"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
        }
        proxy_db_conn = {
            "host": os.environ["PROXY_HOST"],
            **base_db_conn
        }
        db_conn = {
            "host": os.environ["DB_HOST"],
            **base_db_conn
        }
        config = cls(
            ENVIRONMENT=os.environ["ENVIRONMENT"],
            ACCESS_KEY_ID=os.environ["ACC_K_ID"],
            SECRET_ACCESS_KEY=os.environ["ASC_KY"],
            PRICE_DUMP_BUCKET=os.environ["PRICE_DUMP_BUCKET"],
            TAXON_PRICE_DUMP_BUCKET=os.environ["TAXON_PRICE_DUMP_BUCKET"],
            NFT_DUMP_BUCKET=os.environ["NFT_DUMP_BUCKET"],
            NFTS_SHEET_URL=os.environ["NFTS_SHEET_URL"],
            RAW_DUMP_BUCKET=os.environ["CSV_DUMP_BUCKET"],
            DATA_DUMP_BUCKET=os.environ["DATA_DUMP_BUCKET"],
            TWITTER_API_KEY = os.environ["TWITTER_API_KEY"],
            TWITTER_API_SECRET = os.environ["TWITTER_API_SECRET"],
            STAGE = os.environ["STAGE"],
            PROXY_CONN_INFO=proxy_db_conn,
            DB_CONN_INFO=db_conn,
            WRITE_PROXY=os.environ["WRITE_PROXY_HOST"]
        )
        return config
