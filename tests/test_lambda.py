import pytest
import time
from sls_lambda import NFTokenPriceDump, IssuerPriceDump, NFTaxonDump, NFTokenDump
from utilities import factory


def test_token_price_dump():
    start = time.time()
    runner = NFTokenPriceDump(factory)
    runner.run()
    print(f"Executed in {time.time() - start}")

@pytest.mark.asyncio
async def test_nft_token_dumps():
    start = time.time()
    token_dump_runner = NFTokenDump(factory)
    taxon_dump_runner = NFTaxonDump(factory)
    await token_dump_runner.run()
    await taxon_dump_runner.run()
    print(f"Executed in {time.time() - start}")
