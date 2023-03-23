from sls_lambda import CSVDump, TableDump, TwitterDump, GraphDumps
from utilities import factory
from sls_lambda.invokers import invoke_graph_dump, invoke_twitter_dump, invoke_table_dump
import logging
import asyncio


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def csv_dump(event, context):
    runner = CSVDump(factory)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner.run())
    loop.run_until_complete(asyncio.gather(*[invoke_table_dump(factory.config), invoke_graph_dump(factory.config), invoke_twitter_dump(factory.config)]))


def table_dump(event, context):
    runner = TableDump(factory)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner.run())


def graph_dump(event, context):
    runner = GraphDumps(factory)
    runner.run()


def twitter_dump(event, context):
    runner = TwitterDump(factory)
    runner.run()


# def test_db_pool(event, context):
#     db_connect = DataBaseConnector(Config)
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(db_connect.create_db_pool())
#
#
#
# def issuers_nft_dumps(event, context):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(dump_issuers_nfts())
#
#
# def issuers_taxon_dumps(event, context):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(dump_issuers_taxons())
#
#
# def tokens_price_dump(event, context):
#     issuer = event["issuer"]
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(pricing.dump_issuer_taxons_offer(issuer))
#
#
# def issuer_price_dump(event, context):
#     issuer = event["issuer"]
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(pricing.dump_issuer_pricing(issuer))
#
#
# def tracked_issuers_token_price_dumps(event, context):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(pricing.dump_issuers_taxons_offer())
#     invoke_issuers_pricing_dump()
#
# def tracked_issuers_price_dumps(event, context):
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(pricing.dump_issuers_pricing())
#     invoke_csv_dump()