import asyncio
import datetime
import logging
from io import StringIO

import numpy as np
import pandas as pd

from utilities import (file_to_time, get_day_df, get_monthly_df, get_pct,
                       get_s3_resource, get_weekly_df, read_df, write_df, read_json, chunks)

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

    async def _run(self):
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
            if col == "Market_Cap":
                await self.writer.write_df(
                    df_new,
                    f"xls20/latest/{col}_Graph.json",
                    "json",
                )
                day_df = get_day_df(df, 24)  # noqa
                week_df = get_weekly_df(df, 168)
                month_df = get_monthly_df(df, 672)
                day_df["x"] = day_df["x"] * 1000
                week_df["x"] = week_df["x"] * 1000
                month_df["x"] = month_df["x"] * 1000
                await self.writer.write_df(
                    df,
                    f"xls20/history/{int(latest_unix)}/{col}_Graph.json",
                    "json"
                )
                await self.writer.write_df(
                    day_df,
                    f"xls20/latest/{col}_Graph_Day.json",
                    "json"
                )
                await self.writer.write_df(
                    week_df,
                    f"xls20/latest/{col}_Graph_Week.json",
                    "json"
                )
                await self.writer.write_df(
                    month_df,
                    f"xls20/latest/{col}_Graph_Month.json",
                    "json"
                )
            elif col == "Holder_Count":
                day_df = get_day_df(df, 24)
                await self.writer.write_df(
                    day_df,
                    f"xls20/latest/{col}_Graph.json",
                    "json"
                )
            else:
                week_df = get_day_df(df, 168)
                await self.writer.write_df(
                    week_df,
                    f"xls20/latest/{col}_Graph.json",
                    "json"
                )
            buffer = StringIO()
            buffer.write(pct)
            s3 = get_s3_resource(self.factory.config)
            s3.Object(
                self.factory.config.DATA_DUMP_BUCKET,
                f"xls20/history/{int(latest_unix)}/{col}_Percentage_Change.json",
            ).put(Body=buffer.getvalue())
            s3.Object(
                self.factory.config.DATA_DUMP_BUCKET,
                f"xls20/latest/{col}_Percentage_Change.json",
            ).put(Body=buffer.getvalue())

    def run(self) -> None:
        asyncio.run(self._run())


class TaxonPriceGraph(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("data")

    async def _process_pricing(self, date_time, issuer, taxon):
        date_time_str = date_time.strftime('%Y-%m-%d-%H')
        file_path = f"{issuer}/{taxon}/{date_time_str}.json"
        unix_time_stamp = int(date_time.timestamp() * 1000)
        default_pricing = {"mid_price": 0, "floor_price": 0}
        pricing = await read_json(self.factory.config.TAXON_PRICE_DUMP_BUCKET, file_path, self.factory.config)
        pricing = pricing if pricing is not None else default_pricing
        pricing["mid_price"] = pricing["mid_price"]/1000000
        pricing["floor_price"] = pricing["floor_price"]/1000000
        return {"x": unix_time_stamp, "y": pricing["floor_price"]}

    async def _get_db_projects(self):
        pool = await self.db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT issuer, taxon FROM nft_buy_sell_offers GROUP BY issuer, taxon;"
                )
                result = await cursor.fetchall()
            connection.close()
        return result

    async def _run(self, issuer, taxon):
        hours = sorted([(datetime.datetime.utcnow() - datetime.timedelta(hours=i)) for i in range(24)])
        data = await asyncio.gather(*[self._process_pricing(date_time, issuer, taxon) for date_time in hours])
        pct = {
            "currentValue": data[-1]["y"],
            "percentageChanges": {
                "day": {
                    "valueDifference": data[-1]["y"] - data[0]["y"],
                    "percentageChange": (data[-1]["y"] - data[0]["y"])/(data[0]["y"]*100) if data[0]["y"] != 0 else 0,
                    "directionOfChange": int(np.sign(data[-1]["y"] - data[0]["y"]))
                }
            }
        }
        await asyncio.gather(*[
            self.writer.write_json(f"xls20/projects/{issuer}_{taxon}_Graph_Day.json", data),
            self.writer.write_json(f"xls20/projects/{issuer}_{taxon}_Percentage_Change.json", pct)
        ])

    async def run(self) -> None:
        db_projects = await self._get_db_projects()
        for chunk in chunks(db_projects, 10):
            await asyncio.gather(*[self._run(issuer, taxon) for (issuer, taxon) in chunk])


class NFTSalesGraph(BaseLambdaRunner):
    def __init__(self, factory):
        super().__init__(factory)
        self._set_writer("data")

    async def _get_sales_json(self, path: str) -> dict:
        hour = path.replace(".json", "")
        timestamp = datetime.datetime.strptime(hour, '%Y-%m-%d-%H').timestamp()
        sales = await read_json(self.factory.config.DATA_DUMP_BUCKET, f"sales/{path}", self.factory.config)
        if sales is None:
            return {"timestamp": int(timestamp), "hour": hour, "sales": 0}
        sales["timestamp"] = timestamp
        return sales

    async def _get_sales_df(self, sales_files):
        sales_data = []
        for chunk in chunks(sales_files, 100):
            chunk_sales = await asyncio.gather(*[self._get_sales_json(file) for file in chunk])
            sales_data.extend(chunk_sales)
        sales_df = pd.DataFrame(sales_data)
        return sales_df

    async def _run(self) -> None:
        sales_files = sorted(
            [
                f"{(datetime.datetime.utcnow() - datetime.timedelta(hours=i)).strftime('%Y-%m-%d-%H')}.json"
                for i in range(168)
            ]
        )
        current = sales_files[-1].replace(".json", "")
        latest_date = datetime.datetime.strptime(current, '%Y-%m-%d-%H')
        latest_unix = latest_date.timestamp()

        sales_df = await self._get_sales_df(sales_files)
        sales_df["x"] = sales_df["timestamp"]
        sales_df["y"] = sales_df["sales"]
        graph_df = sales_df[["x", "y"]]
        week_graph_df = get_weekly_df(graph_df, 24)
        pct = get_pct(graph_df, latest_unix)
        week_graph_df["x"] = week_graph_df["x"] * 1000
        await self.writer.write_df(week_graph_df, "xls20/latest/Sales_Count_Graph.json", "json")
        buffer = StringIO()
        buffer.write(pct)
        s3 = get_s3_resource(self.factory.config)
        s3.Object(
            self.factory.config.DATA_DUMP_BUCKET,
            "xls20/latest/Sales_Count_Percentage_Change.json",
        ).put(Body=buffer.getvalue())

    def run(self) -> None:
        asyncio.run(self._run())
