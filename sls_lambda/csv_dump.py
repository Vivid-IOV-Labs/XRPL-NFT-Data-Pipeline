import asyncio
import datetime
from typing import List, Tuple

import pandas as pd

from utilities import read_json

from .base import BaseLambdaRunner


class CSVDump(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("csv")

    async def _get_supply_volume_df(self, last_hour: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        supply, volume = await asyncio.gather(*[read_json(
            self.factory.config.NFT_DUMP_BUCKET, "supply.json", self.factory.config
        ), read_json(
            self.factory.config.TAXON_PRICE_DUMP_BUCKET,
            f"volume/{last_hour}.json",
            self.factory.config,
        )])
        supply_df = pd.DataFrame(supply)
        volume_df = pd.DataFrame(volume)
        return supply_df, volume_df

    async def _read_price_files_from_s3(self, issuers: List, last_hour: str):
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
        return prices

    def _merge_dfs(  # noqa
            self,
            price_df: pd.DataFrame,
            supply_df: pd.DataFrame,
            volume_df: pd.DataFrame,
            issuers_df: pd.DataFrame
        ) -> pd.DataFrame:
        supply_volume_merge = supply_df.merge(volume_df, how="left", on=["ISSUER"])
        supply_volume_merge = supply_volume_merge.fillna(0)
        merged_1 = price_df.merge(supply_volume_merge, how="inner", on=["ISSUER"])
        final_merge = merged_1.merge(issuers_df, how="inner", on=["ISSUER"])
        return final_merge

    async def _run(self):
        issuers = self.factory.supported_issuers
        last_hour = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
        supply_df, volume_df = await self._get_supply_volume_df(last_hour)
        issuers_df = self.factory.issuers_df
        prices = await self._read_price_files_from_s3(issuers, last_hour)
        price_df = pd.DataFrame([price for price in prices if price is not None])
        supply_df.rename(
            columns={
                "issuer": "ISSUER",
                "supply": "SUPPLY",
                "circulation": "CIRCULATION",
                "holders": "HOLDER_COUNT",
                "tokens_held": "TOKENS_HELD"
            },
            inplace=True,
        )
        volume_df.rename(
            columns={
                "issuer": "ISSUER",
                "volume": "VOLUME",
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
            },
            inplace=True,
        )
        price_df["PRICEXRP"] = price_df["MID_PRICE_XRP"]

        final_merge = self._merge_dfs(price_df, supply_df, volume_df, issuers_df)
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
                "VOLUME",
                "SUPPLY",
                "CIRCULATION",
                "HOLDER_COUNT",
                "TOKENS_HELD"
            ]
        ].copy()
        final_df["MARKET_CAP"] = final_df["SUPPLY"] * final_df["PRICEXRP"]
        final_df["HELD_0"] = final_df["SUPPLY"] - final_df["CIRCULATION"]
        final_df["TWITTER"] = final_df["TWITTER"].str.split("/").str[-1]
        await asyncio.gather(
            *[
                self.writer.write_df(final_df, f"{last_hour}.csv", "csv"),
                self.writer.write_df(price_df, f"{last_hour}_price.csv", "csv"),
            ]
        )

    def run(self) -> None:
        asyncio.run(self._run())
