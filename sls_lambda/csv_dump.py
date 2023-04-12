import asyncio
import datetime

import pandas as pd

from utilities import read_json

from .base import BaseLambdaRunner


class CSVDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("csv")

    async def run(self) -> None:
        issuers = self.factory.supported_issuers
        last_hour = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
        supply = await read_json(
            self.factory.config.NFT_DUMP_BUCKET, "supply.json", self.factory.config
        )
        supply_df = pd.DataFrame(supply)
        issuers_df = self.factory.issuers_df
        prices = await asyncio.gather(
            *[
                read_json(
                    self.factory.config.PRICE_DUMP_BUCKET,
                    f"{last_hour}/{issuer}/price.json",
                    self.factory.config,
                )
                for issuer in issuers
            ]
        )
        price_df = pd.DataFrame([price for price in prices if price is not None])
        supply_df.rename(
            columns={
                "issuer": "ISSUER",
                "supply": "SUPPLY",
                "circulation": "CIRCULATION",
            },
            inplace=True,
        )
        issuers_df.rename(
            columns={
                "Issuer_Account": "ISSUER",
                "Project_Name": "NAME",
                "Website_URL": "WEBSITE",
                "Twitter_URL": "TWITTER",
            },
            inplace=True,
        )
        price_df.rename(
            columns={
                "issuer": "ISSUER",
                "mid_price": "MID_PRICE_XRP",
                "floor_price": "FLOOR_PRICE_XRP",
                "max_buy_offer": "MAX_BUY_OFFER_XRP",
            }, inplace=True
        )
        price_df["PRICEXRP"] = price_df["MID_PRICE_XRP"]
        merged_1 = price_df.merge(supply_df, how="inner", on=["ISSUER"])
        final_merge = merged_1.merge(issuers_df, how="inner", on=["ISSUER"])
        final_df = final_merge[
            [
                "ISSUER",
                "NAME",
                "WEBSITE",
                "TWITTER",
                "PRICEXRP",
                "FLOOR_PRICE_XRP",
                "MID_PRICE_XRP",
                "MAX_BUY_OFFER_XRP",
                "SUPPLY",
                "CIRCULATION",
            ]
        ].copy()
        final_df["MARKET_CAP"] = final_df["SUPPLY"] * final_df["PRICEXRP"]
        final_df["HELD_0"] = final_df["SUPPLY"] - final_df["CIRCULATION"]
        final_df["HOLDER_COUNT"] = final_df["CIRCULATION"]
        final_df["TWITTER"] = final_df["TWITTER"].str.split("/").str[-1]
        await asyncio.gather(
            *[
                self.writer.write_df(final_df, f"{last_hour}.csv", "csv"),
                self.writer.write_df(price_df, f"{last_hour}_price.csv", "csv"),
            ]
        )
