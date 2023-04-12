import asyncio
import sys
import time
from multiprocessing import Process
import logging
from sls_lambda import NFTokenPriceDump, NFTokenDump, IssuerPriceDump, NFTaxonDump, CSVDump, TableDump, TwitterDump, GraphDumps
from utilities import factory


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    section = sys.argv[1]
    start = time.time()
    if section == "token-dump":
        runner = NFTokenDump(factory)
        asyncio.run(runner.run())
    elif section == "taxon-dump":
        runner = NFTaxonDump(factory)
        asyncio.run(runner.run())
    elif section == "taxon-pricing":
        runner = NFTokenPriceDump(factory)
        runner.run()
    elif section == "issuer-pricing":
        runner = IssuerPriceDump(factory)
        asyncio.run(runner.run())
    elif section == "csv-dump":
        runner = CSVDump(factory)
        asyncio.run(runner.run())
    elif section == "table-dump":
        runner = TableDump(factory)
        runner.sync_run()
    elif section == "graph-dump":
        runner = GraphDumps(factory)
        runner.run()
    elif section == "twitter-dump":
        runner = TwitterDump(factory)
        runner.run()
    elif section == "data":
        graph_runner = GraphDumps(factory)
        twitter_runner = TwitterDump(factory)
        table_runner = TableDump(factory)

        p1 = Process(target=graph_runner.run)
        p2 = Process(target=twitter_runner.run)
        p3 = Process(target=table_runner.sync_run)
        p1.start()
        p2.start()
        p3.start()
        p1.join()
        p2.join()
        p3.join()
    else:
        logger.info("Invalid Option. Available options are `token-dump, taxon-dump, taxon-pricing, issuer-pricing, table-dump`")
    logger.info(f"Executed in {time.time() - start}")
