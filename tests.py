import logging
import time
import asyncio
from handlers import dump_issuer_taxon_offers


logger = logging.getLogger("app_log")
file_handler = logging.FileHandler("logger.log")
logger.addHandler(file_handler)

async def test():
    start = time.monotonic()
    # await dump_issuers_nfts()
    # await dump_issuers_taxons()
    await dump_issuer_taxon_offers("r9ZW5tjbhKFLWxs4j1KqF61YSHAyDvo52D")
    print(f"Executed in {time.monotonic() - start}\n\n")

asyncio.run(test())
