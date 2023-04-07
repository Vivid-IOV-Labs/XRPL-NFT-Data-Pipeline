import asyncio
import time
import logging
from sls_lambda import IssuerPriceDump
from sls_lambda.invokers import invoke_csv_dump, invoke_taxon_dumps, invoke_token_dumps
from utilities import factory


logger = logging.getLogger("app_log")
formatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

async def token_taxon_invoker():
    await asyncio.gather(*[invoke_token_dumps(factory.config), invoke_taxon_dumps(factory.config)])

def xls20_data_pipeline():
    # taxon_price_runner = NFTokenPriceDump(factory)
    issuer_price_runner = IssuerPriceDump(factory)
    start = time.time()
    # asyncio.run(token_taxon_invoker())

    # taxon_price_runner.run()
    asyncio.run(issuer_price_runner.run())

    invoke_csv_dump(factory.config)

    logger.info(f"Task Completed In {time.time() - start} seconds...")

if __name__ == "__main__":
    xls20_data_pipeline()
