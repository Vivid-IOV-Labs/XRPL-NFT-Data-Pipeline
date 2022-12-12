from main import dump_issuers_nfts, dump_issuers_taxons, dump_issuer_taxon_offers
import logging
import asyncio

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

