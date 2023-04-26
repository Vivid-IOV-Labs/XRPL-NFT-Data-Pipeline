import os
from dataclasses import dataclass


try:
    from dotenv import load_dotenv  # noqa
    load_dotenv(".env")
except ModuleNotFoundError:
    pass


@dataclass
class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "PROD")
    ACCESS_KEY_ID = os.getenv("ACC_K_ID", "AKIAVK246LUVTD5H6MRY")
    SECRET_ACCESS_KEY = os.getenv("ASC_KY", "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")
    PRICE_DUMP_BUCKET = os.getenv("PRICE_DUMP_BUCKET", "python-xls20-pricing")
    NFT_DUMP_BUCKET = os.getenv("NFT_DUMP_BUCKET", "xls20-issuer-nfts")
    NFTS_SHEET_URL = os.getenv("NFT_SHEETS_URL", "https://docs.google.com/spreadsheets/d/1macRWPzlVyRJcssG8v3VxFBlSOemJ3zUpnAPSZlsy-w/export?format=csv&gid=1175574524")
    RAW_DUMP_BUCKET = os.getenv("CSV_DUMP_BUCKET", "peerkat-xls20-data-dumps-raw")
    DATA_DUMP_BUCKET = os.getenv("DATA_DUMP_BUCKET", "test-data-dumps")
    TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "W7ELEojTfblc304cq99tJbDGl")
    TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET", "2NWrwduhunhfbSqF2c7Es5e9bBW5fwN0zjGOXbYPdqjaDqsKc4")
    STAGE = os.getenv("STAGE", "sql-migrate")
    DB_BASE_CONN_INFO = {
        "port": os.getenv("RDS_PORT", "5432"),
        "database": os.getenv("DB_NAME", "peerkat_xrpl_main"),
        "user": os.getenv("RDS_USER", "postgres"),
        "password": os.getenv("RDS_PASSWORD", "DvbgU8KYlhhG4s13L45i")
    }
    PROXY_CONN_INFO = {
        "host": os.getenv("PROXY_HOST", "aurora-proxy-read-only.endpoint.proxy-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"),
        **DB_BASE_CONN_INFO
    }
