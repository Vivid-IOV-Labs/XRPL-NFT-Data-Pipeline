import time

import pytest

from sls_lambda import IssuerPriceDump, NFTaxonDump, NFTokenDump
from utilities import factory


# def test_token_price_dump():
#     start = time.time()
#     runner = NFTokenPriceDump(factory)
#     runner.run()
#     print(f"Executed in {time.time() - start}")


def test_issuer_price_dump():
    start = time.time()
    runner = IssuerPriceDump(factory)
    runner.run()
    print(f"Executed in {time.time() - start}")


@pytest.mark.asyncio
async def test_token_dumps():
    start = time.time()
    token_dump_runner = NFTokenDump(factory)
    await token_dump_runner.run()
    print(f"Executed in {time.time() - start}")


@pytest.mark.asyncio
async def test_taxon_dumps():
    start = time.time()
    taxon_dump_runner = NFTaxonDump(factory)
    await taxon_dump_runner.run()
    print(f"Executed in {time.time() - start}")
