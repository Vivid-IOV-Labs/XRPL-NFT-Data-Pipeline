import asyncio
import sys
import time
import logging
from sls_lambda import NFTokenPriceDump, NFTokenDump, IssuerPriceDump, NFTaxonDump
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
        runner.run()
    else:
        logger.info("Invalid Option. Available options are `token-dump, taxon-dump, taxon-pricing, issuer-pricing`")
    logger.info(f"Executed in {time.time() - start}")
