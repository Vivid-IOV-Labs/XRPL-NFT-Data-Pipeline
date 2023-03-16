import logging
import time
import asyncio
import sys
from main import factory
from csv_dump import xls20_csv_dump
from pricing import PricingV2
from urllib.parse import quote_plus, quote
import aioboto3
import aiopg
from graph import graph
from table import table
from twitter import twitter


logger = logging.getLogger("app_log")
file_handler = logging.FileHandler("logger.log")
logger.addHandler(file_handler)



async def aiotest():
    start = time.monotonic()  # noqa
    issuers = factory.supported_issuers
    pricing = PricingV2()
    await asyncio.gather(*[pricing.dump_issuer_taxons_offer(issuer) for issuer in ["rLtgE7FjDfyJy5FGY87zoAuKtH6Bfb9QnE"]])
    print(f"Executed in {time.monotonic() - start}\n\n")



if __name__ == "__main__":
    if sys.argv[1] == "async":
        asyncio.run(aiotest())
    elif sys.argv[1] == "sync":
        start = time.monotonic()
        graph()
        twitter()
        print(f"Executed in {time.monotonic() - start}\n\n")
    else:
        logger.error("Pass which test to run")
