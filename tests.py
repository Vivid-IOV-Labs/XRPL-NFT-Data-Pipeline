import logging
import time
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from main import dump_issuer_taxon_offers, invoke_issuer_pricing_dump, factory, dump_issuers_taxons, dump_issuers_nfts, xls20_raw_data_dump, invoke_csv_dump, invoke_table_dump
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
    await dump_issuer_taxon_offers("r4zG9kcxyvq5niULmHbhUUbfh9R9nnNBJ4")
    # await xls20_raw_data_dump()
    # await table()
    print(f"Executed in {time.monotonic() - start}\n\n")


def thread_test():
    start = time.monotonic()  # noqa
    issuers = factory.supported_issuers
    with ThreadPoolExecutor(max_workers=len(issuers)) as executor:
        executor.map(invoke_issuer_pricing_dump, issuers)
    print(f"Executed in {time.monotonic() - start}\n\n")


if __name__ == "__main__":
    if sys.argv[1] == "async":
        asyncio.run(aiotest())
    elif sys.argv[1] == "sync":
        start = time.monotonic()
        graph()
        print(f"Executed in {time.monotonic() - start}\n\n")
    elif sys.argv[1] == "thread":
        thread_test()
    elif sys.argv[1] == "inv":
        invoke_table_dump()
        invoke_csv_dump()
    else:
        twitter()
        # import tweepy
        #
        # auth = tweepy.OAuth2AppHandler("W7ELEojTfblc304cq99tJbDGl", "2NWrwduhunhfbSqF2c7Es5e9bBW5fwN0zjGOXbYPdqjaDqsKc4")
        # api = tweepy.API(auth)
        #
        # tweets = api.user_timeline(screen_name="BearableguyClub", count=100)
        # for tweet in tweets:
        #     print()
        #     print("\n")
        logger.error("Pass which test to run")
