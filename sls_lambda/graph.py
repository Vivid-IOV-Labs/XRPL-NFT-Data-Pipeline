import asyncio
import datetime
import logging
from io import StringIO

import numpy as np
import pandas as pd

from utilities import (file_to_time, get_day_df, get_monthly_df, get_pct,
                       get_s3_resource, get_weekly_df, read_df, write_df)

from .base import BaseLambdaRunner

logger = logging.getLogger("app_log")


class GraphDumps(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("data")

    def _process_file(self, path, df_dic, price_df, num_col):
        try:
            unix_time = int(file_to_time(path).timestamp())
            df = read_df(f"s3://{self.factory.config.RAW_DUMP_BUCKET}/{path}")
            df = pd.merge(df, price_df, on="Issuer", how="outer")
            df["Market_Cap"] = df["Circulation"] * df["Price"] / 1000000
            tot = df.select_dtypes(np.number).sum()
            for col in num_col:
                df_dic[col].append([unix_time, tot[col]])
            return df_dic
        except FileNotFoundError:
            logger.info(f"file not found for {path}")

    def run(self) -> None:
        files = sorted(
            [
                f"{(datetime.datetime.utcnow() - datetime.timedelta(hours=i)).strftime('%Y-%m-%d-%H')}.csv"
                for i in range(720)
            ]
        )
        current = files[-1]
        latest_date = file_to_time(current)
        latest_unix = latest_date.timestamp()
        num_col = ["Holder_Count", "Market_Cap", "Supply"]
        dic = {}
        for col in num_col:
            dic[col] = []
        price_df = pd.read_csv(
            f"s3://{self.factory.config.RAW_DUMP_BUCKET}/{current.replace('.csv', '_price.csv')}"
        )
        price_df.columns = [
            "Issuer",
            "Floor_Price_XRP",
            "Max_Buy_Offer_XRP",
            "Mid_Price_XRP",
            "Price",
        ]
        for file in files:
            self._process_file(file, dic, price_df, num_col)

        for col in num_col:
            df = pd.DataFrame(dic[col], columns=["x", "y"])
            df_new = df.copy()
            df_new["x"] = df_new["x"] * 1000
            pct = get_pct(df, latest_unix)
            if self.factory.config.ENVIRONMENT == "LOCAL":
                write_df(df_new, f"data/json_dumps/{col}_Graph.json", "json")
                if col == "Market_Cap":
                    day_df = get_day_df(df, 24)  # noqa
                    week_df = get_weekly_df(df, 168)
                    month_df = get_monthly_df(df, 672)
                    day_df["x"] = day_df["x"] * 1000
                    week_df["x"] = week_df["x"] * 1000
                    month_df["x"] = month_df["x"] * 1000
                    write_df(day_df, f"data/json_dumps/{col}_Graph_Day.json", "json")
                    write_df(week_df, f"data/json_dumps/{col}_Graph_Week.json", "json")
                    write_df(
                        month_df, f"data/json_dumps/{col}_Graph_Month.json", "json"
                    )
                with open(f"data/json_dumps/{col}_Percentage_Change.json", "w") as file:
                    file.write(pct)
            else:
                if col == "Market_Cap":
                    write_df(
                        df_new,
                        f"xls20/latest/{col}_Graph.json",
                        "json",
                        bucket=self.factory.config.DATA_DUMP_BUCKET,
                    )
                    day_df = get_day_df(df, 24)  # noqa
                    week_df = get_weekly_df(df, 168)
                    month_df = get_monthly_df(df, 672)
                    day_df["x"] = day_df["x"] * 1000
                    week_df["x"] = week_df["x"] * 1000
                    month_df["x"] = month_df["x"] * 1000
                    write_df(
                        df,
                        f"xls20/history/{int(latest_unix)}/{col}_Graph.json",
                        "json",
                        bucket=self.factory.config.DATA_DUMP_BUCKET,
                    )
                    write_df(
                        day_df,
                        f"xls20/latest/{col}_Graph_Day.json",
                        "json",
                        bucket=self.factory.config.DATA_DUMP_BUCKET,
                    )
                    write_df(
                        week_df,
                        f"xls20/latest/{col}_Graph_Week.json",
                        "json",
                        bucket=self.factory.config.DATA_DUMP_BUCKET,
                    )
                    write_df(
                        month_df,
                        f"xls20/latest/{col}_Graph_Month.json",
                        "json",
                        bucket=self.factory.config.DATA_DUMP_BUCKET,
                    )
                else:
                    week_df = get_day_df(df, 168)
                    write_df(
                        week_df,
                        f"xls20/latest/{col}_Graph.json",
                        "json",
                        bucket=self.factory.config.DATA_DUMP_BUCKET,
                    )
                buffer = StringIO()
                buffer.write(pct)
                s3 = get_s3_resource()
                s3.Object(
                    self.factory.config.DATA_DUMP_BUCKET,
                    f"xls20/history/{int(latest_unix)}/{col}_Percentage_Change.json",
                ).put(Body=buffer.getvalue())
                s3.Object(
                    self.factory.config.DATA_DUMP_BUCKET,
                    f"xls20/latest/{col}_Percentage_Change.json",
                ).put(Body=buffer.getvalue())
