import pandas as pd

from sls_lambda import CSVDump, NFTaxonDump, NFTokenDump
from utilities import Factory
import json

def test_csv_dump(setup):
    config = setup["config"]
    factory = Factory(config)
    cwd = setup["cwd"]
    last_hour = setup["last_hour"]

    runner = CSVDump(factory)
    runner.run()

    csv_df = pd.read_csv(f"{cwd}/data/test/{last_hour}.csv")
    price_df = pd.read_csv(f"{cwd}/data/test/{last_hour}_price.csv")
    expected_csv_columns = [
        "ISSUER",
        "NAME",
        "WEBSITE",
        "TWITTER",
        "PRICEXRP",
        "FLOOR_PRICE_XRP",
        "MID_PRICE_XRP",
        "MAX_BUY_OFFER_XRP",
        "VOLUME",
        "SUPPLY",
        "CIRCULATION",
        "HOLDER_COUNT",
        "TOKENS_HELD",
        "MARKET_CAP",
        "HELD_0",
    ]
    expected_price_columns = [
        "ISSUER",
        "FLOOR_PRICE_XRP",
        "MAX_BUY_OFFER_XRP",
        "MID_PRICE_XRP",
        "PRICEXRP"
    ]
    assert all(column in csv_df.columns for column in expected_csv_columns)
    assert all(column in price_df.columns for column in expected_price_columns)
    assert len(csv_df) == len(factory.supported_issuers)
    assert len(price_df) == len(factory.supported_issuers)


def test_nft_dumps(setup):
    config = setup["config"]
    cwd = setup["cwd"]
    last_hour = setup["last_hour"]
    factory = Factory(config)
    factory._supported_issuers = factory._supported_issuers[:5]
    nft_dump_runner = NFTokenDump(factory)
    nft_dump_runner.run()
    nft_taxon_dump_runner = NFTaxonDump(factory)
    nft_taxon_dump_runner.run()

    # Check for nft-token-dump files
    for issuer in factory.supported_issuers:
        data = json.load(open(f"{cwd}/data/test/{last_hour}/{issuer}.json"))
        assert type(data["nfts"]) == list
        assert type(data["issuer"]) == str
    # Check for supply.json dump
    supply_dump = json.load(open(f"{cwd}/data/test/supply.json"))
    assert len(supply_dump) == len(factory.supported_issuers)
    # Check for taxon-dump file
    taxon_dump = json.load(open(f"{cwd}/data/test/taxon.json"))
    assert len(taxon_dump) == len(factory.supported_issuers)
