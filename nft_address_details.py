import json
import sys
import time
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json

import aiohttp
import pandas as pd
import asyncio

from utilities import LocalFileWriter, chunks

# snowflake.connector.paramstyle = "qmark"


writer = LocalFileWriter()
ADDRESS_FILE = "data/addresses.csv"

async def fetch_address_details_from_bithomp_api(address: str):
    async with aiohttp.ClientSession(headers={"x-bithomp-token": "e2079870-7242-11ed-95dd-ab1ab9ffd825"}) as session:  # noqa
        url = f"https://bithomp.com/api/v2/address/{address}%username=true&service=true&verifiedDomain=true&inception=true&ledgerInfo=true"
        async with session.get(url) as response:  # noqa
            if response.status == 200:
                content = await response.content.read()
                to_dict = json.loads(content)
                return to_dict
            else:
                content = await response.content.read()
                print(f"Error Fetching address Details: {content}")

async def dump():
    addresses = pd.read_csv(ADDRESS_FILE)["Addresses"].to_list()
    final_data = []
    try:
        fetched_addresses = json.load(open("data/local/addresses/fetched.json", "r"))
        final_data = json.load(open("data/local/addresses/details.json", "r"))
    except FileNotFoundError as e:
        print(e)
        fetched_addresses = []
        final_data = []
    to_fetch = list(set(addresses) - set(fetched_addresses))
    for chunk in chunks(to_fetch, 100):
        details = await asyncio.gather(*[fetch_address_details_from_bithomp_api(address) for address in chunk])
        fetched_addresses.extend(chunk)
        final_data.extend(details)
        await writer.write_json("addresses/fetched.json", fetched_addresses)
        await writer.write_json("addresses/details.json", final_data)
        print("sleeping for 60s ...")
        time.sleep(60)


def create_snowflake_connection():
    conn = snowflake.connector.connect(
        user='TOBI',
        password='Kelechi@2000',
        account='im75570.eu-west-2.aws',
        warehouse='USER_REPORT_WH',
        database='XRPL_USER_INFO',
        schema='PUBLIC',
        role='USERREPORTADMIN'
    )
    return conn

def create_snowflake_table(connection):
    cursor = connection.cursor()
    create_table_sql = '''
        CREATE TABLE xrpl_address_details (
            address STRING,
            xaddress STRING,
            inception BIGINT,
            initial_balance BIGINT,
            genesis BOOLEAN,
            service STRING,
            username STRING,
            verified_domain STRING,
            ledger_info STRING
        )
    '''
    cursor.execute(create_table_sql)
    connection.commit()
    cursor.close()
    connection.close()

def upload_to_snowflake(data, connection, table):
    loaded = json.load(open("data/local/addresses/loaded.json", "r"))
    cursor = connection.cursor()
    for obj in data:
        if loaded.get(obj["address"]) is True:
            print(f"Already Loaded: {obj['address']}")
            continue
        try:
            cursor.execute(
                f"""
                INSERT INTO {table} (
                    address,
                    xaddress,
                    inception,
                    initial_balance,
                    genesis,
                    ledger_info,
                    service,
                    username,
                    verified_domain
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    obj['address'],
                    obj['xAddress'],
                    obj['inception'],
                    obj['initial_balance'],
                    obj['genesis'],
                    obj['ledger_info'],
                    obj['service'],
                    obj['username'],
                    obj['verified_domain'],
                ),
            )
            loaded[obj["address"]] = True
            print(f"Completed for address: {obj['address']}")
        except Exception as e:
            print(e)
    asyncio.run(writer.write_json("addresses/loaded.json", loaded))
    connection.commit()
    cursor.close()
    connection.close()



if __name__ == "__main__":
    arg = sys.argv[1]

    connection = create_snowflake_connection()

    if arg == "create-table":
        create_snowflake_table(connection)
    elif arg == "dump-data":
        asyncio.run(dump())
    elif arg == "load-data":
        num_cols = ["inception", "initial_balance", "genesis"]
        df = pd.read_json("data/local/addresses/details.json")
        columns = {
            "initialBalance": "initial_balance",
            "verifiedDomain": "verified_domain",
            "ledgerInfo": "ledger_info"
        }
        df.rename(columns=columns, inplace=True)
        df["ledger_info"] = df["ledger_info"].apply(lambda x: json.dumps(x))
        df[num_cols] = df[num_cols].fillna(0)
        df = df.fillna("")
        data = df.to_dict(orient="records")
        upload_to_snowflake(data, connection, "XRPL_ADDRESS_DETAILS")
    else:
        print("invalid argument")
