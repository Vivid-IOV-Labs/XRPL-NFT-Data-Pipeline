import logging
import time
import asyncio
import sys
from main import factory
from csv_dump import xls20_csv_dump
from pricing import Pricing
from graph import graph
from table import table
from twitter import twitter


logger = logging.getLogger("app_log")
file_handler = logging.FileHandler("logger.log")
logger.addHandler(file_handler)

async def aiotest():
    start = time.monotonic()  # noqa
    # issuers = factory.supported_issuers
    # for issuer in issuers:
    #     await dump_issuer_taxon_offers(issuer)
    # await dump_issuers_nfts()
    # await dump_issuers_taxons()
    # pricing = Pricing()
    # await pricing.dump_issuer_pricing("rLtgE7FjDfyJy5FGY87zoAuKtH6Bfb9QnE")
    # await pricing.dump_issuers_pricing()
    # await pricing.dump_issuer_taxons_offer("r3a82jDJdg4TyUMEPEH4Wpg62HniXA4Jcj")
    # await pricing.dump_issuers_taxons_offer()
    # await pricing.dump_issuers_taxons_offer()
    # await pricing._dump_issuer_taxons_offer("r4zG9kcxyvq5niULmHbhUUbfh9R9nnNBJ4")
    # await pricing._dump_issuer_pricing("r4zG9kcxyvq5niULmHbhUUbfh9R9nnNBJ4")
    # await xls20_csv_dump()
    await table()
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
