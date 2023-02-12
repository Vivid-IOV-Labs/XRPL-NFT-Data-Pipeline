from main import (
    dump_issuers_nfts,
    dump_issuers_taxons,
    factory,
    xls20_raw_data_dump,
    invoke_csv_dump
)
from pricing import Pricing
from table import table
from graph import graph
from twitter import twitter
import logging
import asyncio

# from concurrent.futures import ThreadPoolExecutor
from threading import Thread

logger = logging.getLogger("app_log")

pricing = Pricing()


def issuers_nft_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump_issuers_nfts())


def issuers_taxon_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump_issuers_taxons())


def tokens_price_dump(event, context):
    issuer = event["issuer"]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pricing.dump_issuer_taxons_offer(issuer))


def tracked_issuers_token_price_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pricing.dump_issuers_taxons_offer())



# def issuer_taxon_average_price_dumps(event, context):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(dump_issuer_taxon_offers(event["issuer"]))


# def raw_data_dump(event, context):
#     logger.info("started")
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(xls20_raw_data_dump())
#
#
# def table_dump(event, context):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(table())
#
#
# def graph_dump(event, context):
#     graph()
#
#
# def twitter_dump(event, context):
#     twitter()
#
#
# def issuer_pricing_invoker(event, context):
#     issuers = factory.supported_issuers
#     for issuer in issuers:
#         print(f"started for Issuer {issuer}")
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(dump_issuer_taxon_offers(issuer))
#         print(f"Completed for issuer {issuer}\n")
#     invoke_csv_dump()
