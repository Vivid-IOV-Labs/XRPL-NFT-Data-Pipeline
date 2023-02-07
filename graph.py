import datetime
import pandas as pd
import numpy as np
from io import StringIO

from config import Config
from utils import file_to_time, read_df, get_pct, write_df, get_s3_resource, get_day_df, get_weekly_df, get_monthly_df

def graph():
    # current_time = datetime.datetime.utcnow()
    # # day_ago = current_time - datetime.timedelta(days=1)
    # current = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')

    files = sorted([f"{(datetime.datetime.utcnow()-datetime.timedelta(hours=i)).strftime('%Y-%m-%d-%H')}.csv" for i in range(720)])
    current = files[-1]
    latest_date = file_to_time(current)
    latest_unix = latest_date.timestamp()
    num_col = ["Holder_Count", "Market_Cap", "Supply"]
    dic = {}
    for col in num_col:
        dic[col] = []
    price_df = pd.read_csv(f"s3://{Config.RAW_DUMP_BUCKET}/{current.replace('.csv', '_price.csv')}")
    price_df.columns = ["Issuer", "Price", "Price_XRP"]
    for file in files:
        try:
            unix_time = int(file_to_time(file).timestamp())
            df = read_df(f"s3://{Config.RAW_DUMP_BUCKET}/{file}")
            df = pd.merge(df, price_df, on="Issuer", how="outer")
            df["Market_Cap"] = df["Circulation"] * df["Price"]
            tot = df.select_dtypes(np.number).sum()
            for col in num_col:
                dic[col].append([unix_time, tot[col]])
        except FileNotFoundError:
            print(f"file mot found for {file}")
            continue

    for col in num_col:
        df = pd.DataFrame(dic[col], columns=["x", "y"])
        df_new = df.copy()
        df_new["x"] = df_new["x"] * 1000
        pct = get_pct(df, latest_unix)
        if Config.ENVIRONMENT == "LOCAL":
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
                write_df(month_df, f"data/json_dumps/{col}_Graph_Month.json", "json")
            with open(f"data/json_dumps/{col}_Percentage_Change.json", "w") as file:
                file.write(pct)
        else:
            if col == "Market_Cap":
                write_df(df_new, f"xls20/latest/{col}_Graph.json", "json", bucket=Config.DATA_DUMP_BUCKET)
                day_df = get_day_df(df, 24)  # noqa
                week_df = get_weekly_df(df, 168)
                month_df = get_monthly_df(df, 672)
                day_df["x"] = day_df["x"] * 1000
                week_df["x"] = week_df["x"] * 1000
                month_df["x"] = month_df["x"] * 1000
                write_df(df, f"xls20/history/{int(latest_unix)}/{col}_Graph.json", "json", bucket=Config.DATA_DUMP_BUCKET)
                write_df(day_df, f"xls20/latest/{col}_Graph_Day.json", "json", bucket=Config.DATA_DUMP_BUCKET)
                write_df(week_df, f"xls20/latest/{col}_Graph_Week.json", "json", bucket=Config.DATA_DUMP_BUCKET)
                write_df(month_df, f"xls20/latest/{col}_Graph_Month.json", "json", bucket=Config.DATA_DUMP_BUCKET)
            else:
                week_df = get_day_df(df, 168)
                write_df(week_df, f"xls20/latest/{col}_Graph.json", "json", bucket=Config.DATA_DUMP_BUCKET)
            buffer = StringIO()
            buffer.write(pct)
            s3 = get_s3_resource()
            s3.Object(Config.DATA_DUMP_BUCKET, f"xls20/history/{int(latest_unix)}/{col}_Percentage_Change.json").put(
                Body=buffer.getvalue()
            )
            s3.Object(Config.DATA_DUMP_BUCKET, f"xls20/latest/{col}_Percentage_Change.json").put(Body=buffer.getvalue())
    return {"statusCode": 200}