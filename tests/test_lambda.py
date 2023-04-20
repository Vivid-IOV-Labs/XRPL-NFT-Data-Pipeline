import time

import pytest

from sls_lambda import IssuerPriceDump, TaxonPriceDump, NFTokenDump, NFTaxonDump
from utilities import factory

def test_taxon_price_dump(setup):
    start = time.time()
    runner = TaxonPriceDump(setup["factory"])
    runner.run()
    print(f"Executed in {time.time() - start}")

def test_issuer_price_dump(setup):
    start = time.time()
    runner = IssuerPriceDump(setup["factory"])
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
