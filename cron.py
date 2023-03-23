import asyncio
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

def xls20_data_pipeline():
    token_dump_runner = NFTokenDump(factory)
    taxon_dump_runner = NFTaxonDump(factory)
    taxon_price_runner = NFTokenPriceDump(factory)
    issuer_price_runner = IssuerPriceDump(factory)

    asyncio.run(token_dump_runner.run())
    asyncio.run(taxon_dump_runner.run())

    taxon_price_runner.run()
    issuer_price_runner.run()

    logger.info("Task Completed")

xls20_data_pipeline()