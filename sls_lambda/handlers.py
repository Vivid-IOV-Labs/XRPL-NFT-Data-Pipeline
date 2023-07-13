import asyncio
import logging

from sls_lambda import (CSVDump, GraphDumps, IssuerPriceDump, NFTaxonDump, NFTokenDump, TableDump)
from sls_lambda.invokers import (invoke_graph_dump, invoke_table_dump)
from utilities import Factory, Config

logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def issuers_nft_dumps(event, context):
    config = Config.from_env()
    factory = Factory(config)
    runner = NFTokenDump(factory)
    runner.run()


def issuers_taxon_dumps(event, context):
    config = Config.from_env()
    factory = Factory(config)
    runner = NFTaxonDump(factory)
    runner.run()


def issuers_price_dump(event, context):
    config = Config.from_env()
    factory = Factory(config)
    issuer_price_runner = IssuerPriceDump(factory)
    asyncio.run(issuer_price_runner.run())


async def after_csv_dump_invocation(config):
    await asyncio.gather(*[invoke_table_dump(config), invoke_graph_dump(config)])

def csv_dump(event, context):
    config = Config.from_env()
    factory = Factory(config)
    runner = CSVDump(factory)
    runner.run()

    asyncio.run(after_csv_dump_invocation(config))

def table_dump(event, context):
    config = Config.from_env()
    factory = Factory(config)
    runner = TableDump(factory)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner.run())


def graph_dump(event, context):
    config = Config.from_env()
    factory = Factory(config)
    runner = GraphDumps(factory)
    runner.run()
