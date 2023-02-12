import os
from dataclasses import dataclass


try:
    from dotenv import load_dotenv
    load_dotenv(".env")
except ModuleNotFoundError:
    pass


@dataclass
class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT")
    ACCESS_KEY_ID = os.getenv("ACC_K_ID")
    SECRET_ACCESS_KEY = os.getenv("ASC_KY")
    PRICE_DUMP_BUCKET = os.getenv("PRICE_DUMP_BUCKET")
    NFT_DUMP_BUCKET = os.getenv("NFT_DUMP_BUCKET")
    XRPL_SERVERS = {"https://xrplcluster.com", "https://s1.ripple.com:51234", "https://s2.ripple.com:51234"}
    NFTS_SHEET_URL = os.getenv("NFT_SHEETS_URL")
    RAW_DUMP_BUCKET = os.getenv("CSV_DUMP_BUCKET")
    DATA_DUMP_BUCKET = os.getenv("DATA_DUMP_BUCKET")
    TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
    TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
    STAGE = os.getenv("STAGE")
