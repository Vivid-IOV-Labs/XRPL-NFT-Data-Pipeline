import datetime
import pandas as pd
import os

import pytest

from sls_lambda import CSVDump

def test_csv_dump(setup):
    factory = setup["factory"]
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
