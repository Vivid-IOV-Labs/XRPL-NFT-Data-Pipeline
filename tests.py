import logging
import time
import asyncio
from main import factory
from multiprocessing.pool import Pool
from pricing import PricingV2


logger = logging.getLogger("app_log")
file_handler = logging.FileHandler("logger.log")
logger.addHandler(file_handler)

pricing = PricingV2()

# async def aiotest():
#     start = time.monotonic()  # noqa
#     issuers = factory.supported_issuers
#     await asyncio.gather(*[pricing.dump_issuer_taxons_offer(issuer) for issuer in ["rLtgE7FjDfyJy5FGY87zoAuKtH6Bfb9QnE"]])
#     print(f"Executed in {time.monotonic() - start}\n\n")


def spawn_process(issuer: str):
    asyncio.run(pricing.dump_issuer_taxons_offer(issuer))



if __name__ == "__main__":
    start = time.monotonic()  # noqa
    # issuers = factory.supported_issuers
    # with Pool(8) as pool:
    #     pool.map(spawn_process, issuers)
    print(f"Executed in {time.monotonic() - start}\n\n")
