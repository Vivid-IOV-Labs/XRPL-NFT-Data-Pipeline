import asyncio
import logging
import sys
import time

from sls_lambda import (CSVDump, GraphDumps, IssuerPriceDump, NFTaxonDump,
                        NFTokenDump, TaxonPriceGraph, TableDump, TwitterDump, TaxonPriceDump, NFTSalesDump, NFTSalesGraph)
from utilities import Factory, Config
from scripts import

logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

# Entry Point For Pipeline and Scripts [As A CLI]

if __name__ == "__main__":
    section = sys.argv[1]
    start = time.time()

    config = Config.from_env('env/.env.local')
    factory = Factory(config)
    if section == "token-dump":
        runner = NFTokenDump(factory)
        runner.run()
    elif section == "taxon-dump":
        runner = NFTaxonDump(factory)
        runner.run()
    elif section == "issuer-pricing":
        runner = IssuerPriceDump(factory)
        asyncio.run(runner.run())
    elif section == "taxon-pricing":
        runner = TaxonPriceDump(factory)
        runner.run()
    elif section == "csv-dump":
        runner = CSVDump(factory)
        runner.run()
    elif section == "table-dump":
        runner = TableDump(factory)
        runner.sync_run()
    elif section == "graph-dump":
        runner = GraphDumps(factory)
        runner.run()
    elif section == "taxon-price-graph":
        runner = TaxonPriceGraph(factory)
        asyncio.run(runner.run())
    elif section == "twitter-dump":
        runner = TwitterDump(factory)
        runner.run()
    elif section == "sales-dump":
        runner = NFTSalesDump(factory)
        runner.run()
    elif section == "sales-graph":
        runner = NFTSalesGraph(factory)
        runner.run()
    else:
        logger.info(
            "Invalid Option. Available options are `token-dump, taxon-dump, taxon-pricing, issuer-pricing, table-dump`"
        )
    logger.info(f"Executed in {time.time() - start}")
