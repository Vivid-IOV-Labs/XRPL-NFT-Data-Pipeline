from nft_dumps import dump_issuers_nfts, dump_issuers_taxons
from invokers import invoke_issuers_pricing_dump, invoke_csv_dump, invoke_table_dump, invoke_twitter_dump, invoke_graph_dump
from pricing import Pricing
from csv_dump import xls20_csv_dump
from table import table
from graph import graph
from twitter import twitter
import logging
import asyncio


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


def issuer_price_dump(event, context):
    issuer = event["issuer"]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pricing.dump_issuer_pricing(issuer))


def tracked_issuers_token_price_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pricing.dump_issuers_taxons_offer())
    invoke_issuers_pricing_dump()

def tracked_issuers_price_dumps(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pricing.dump_issuers_pricing())
    invoke_csv_dump()


def csv_dump(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(xls20_csv_dump())
    loop.run_until_complete(asyncio.gather(*[invoke_table_dump(), invoke_graph_dump(), invoke_twitter_dump()]))


def table_dump(event, context):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(table())


def graph_dump(event, context):
    graph()


def twitter_dump(event, context):
    twitter()



