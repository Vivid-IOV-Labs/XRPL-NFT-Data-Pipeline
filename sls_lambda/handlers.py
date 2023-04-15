import asyncio
import logging

from sls_lambda import (CSVDump, GraphDumps, IssuerPriceDump, NFTaxonDump,
                        NFTokenDump, TableDump, TwitterDump)
from sls_lambda.invokers import (invoke_graph_dump, invoke_table_dump,
                                 invoke_twitter_dump)
from utilities import factory

logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def issuers_nft_dumps(event, context):
    runner = NFTokenDump(factory)
    asyncio.run(runner.run())


def issuers_taxon_dumps(event, context):
    runner = NFTaxonDump(factory)
    asyncio.run(runner.run())


def issuers_price_dump(event, context):
    issuer_price_runner = IssuerPriceDump(factory)
    asyncio.run(issuer_price_runner.run())


def csv_dump(event, context):
    runner = CSVDump(factory)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner.run())
    loop.run_until_complete(
        asyncio.gather(
            *[
                invoke_table_dump(factory.config),
                invoke_graph_dump(factory.config),
                invoke_twitter_dump(factory.config),
            ]
        )
    )


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
