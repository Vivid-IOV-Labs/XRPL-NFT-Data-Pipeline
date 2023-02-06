from main import (
    dump_issuers_nfts,
    dump_issuers_taxons,
    dump_issuer_taxon_offers,
    factory,
    invoke_issuer_pricing_dump,
    xls20_raw_data_dump,
    invoke_csv_dump,
    chunks,
)
from table import table
from graph import graph
from twitter import twitter
import logging
import asyncio

# from concurrent.futures import ThreadPoolExecutor
from threading import Thread

logger = logging.getLogger("app_log")


def issuers_nft_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump_issuers_nfts())


def issuers_taxon_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump_issuers_taxons())


def issuer_taxon_average_price_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump_issuer_taxon_offers(event["issuer"]))


def raw_data_dump(event, context):
    logger.info("started")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(xls20_raw_data_dump())


def table_dump(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(table())


def graph_dump(event, context):
    graph()


def twitter_dump(event, context):
    twitter()


def issuer_pricing_invoker(event, context):
    issuers = factory.supported_issuers
    for issuer in issuers:
        print(f"started for Issuer {issuer}")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(dump_issuer_taxon_offers(issuer))
        print(f"Completed for issuer {issuer}\n")
    # for chunk in chunks(issuers, 3):
    #     threads = []
    #     for issuer in chunk:
    #         thread = Thread(target=invoke_issuer_pricing_dump, args=(issuer,))
    #         threads.append(thread)
    #         thread.start()
    #     for thread in threads:
    #         thread.join()
    # # with ThreadPoolExecutor(max_workers=len(issuers)) as executor:
    # #     executor.map(invoke_issuer_pricing_dump, issuers)
    invoke_csv_dump()
