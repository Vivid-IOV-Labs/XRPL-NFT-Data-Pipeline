import sys
import time
import snowflake.connector
import json

import aiohttp
import pandas as pd
import asyncio

from utilities import LocalFileWriter, chunks, Config


writer = LocalFileWriter()
ADDRESS_FILE = "data/unique_addresses.csv"
BITHOMP_BASE_URL = "https://bithomp.com/api/v2"

async def fetch_address_details_from_bithomp_api(address: str, cfg: Config):
    async with aiohttp.ClientSession(headers={"x-bithomp-token": cfg.BITHOMP_TOKEN}) as session:  # noqa
        url = f"{BITHOMP_BASE_URL}/address/{address}?username=true&service=true&verifiedDomain=true&inception=true&ledgerInfo=true"
        async with session.get(url) as response:  # noqa
            if response.status == 200:
                content = await response.content.read()
                to_dict = json.loads(content)
                return to_dict if "address" in to_dict else None
            else:
                content = await response.content.read()
                print(f"Error Fetching address Details: {content}")

async def dump(cfg: Config):
    addresses = pd.read_csv(ADDRESS_FILE)["ACCOUNT"].to_list()
    final_data = []
    try:
        final_data = json.load(open("data/local/addresses/details.json", "r"))
        fetched_addresses = [data['address'] for data in final_data]
    except FileNotFoundError as e:
        fetched_addresses = []
        final_data = []
    to_fetch = list(set(addresses) - set(fetched_addresses))
    for chunk in chunks(to_fetch, 10):
        details = await asyncio.gather(*[fetch_address_details_from_bithomp_api(address, cfg) for address in chunk])
        fetched_addresses.extend(chunk)
        final_data.extend([detail for detail in details if detail is not None])
        await writer.write_json("addresses/fetched.json", fetched_addresses)
        await writer.write_json("addresses/details.json", final_data)
        print("sleeping for 60s ...")
        time.sleep(60)


def create_snowflake_connection(cfg: Config):
    conn = snowflake.connector.connect(
        user=cfg.SNOWFLAKE_USER,
        password=cfg.SNOWFLAKE_PASSWORD,
        account=cfg.SNOWFLAKE_ACCOUNT,
        warehouse='USER_REPORT_WH',
        database=cfg.SNOWFLAKE_DB,
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


# if __name__ == "__main__":
#     arg = sys.argv[1]
#     config = Config.from_env(".env")
#     if arg == "create-table":
#         connection = create_snowflake_connection(config)
#         create_snowflake_table(connection)
#     elif arg == "dump-data":
#         asyncio.run(dump(config))
#     elif arg == "load-data":
#         num_cols = ["inception", "initial_balance", "genesis"]
#         df = pd.read_json("data/local/addresses/details.json")
#         columns = {
#             "initialBalance": "initial_balance",
#             "verifiedDomain": "verified_domain",
#             "ledgerInfo": "ledger_info"
#         }
#         df.rename(columns=columns, inplace=True)
#         df["ledger_info"] = df["ledger_info"].apply(lambda x: json.dumps(x))
#         df[num_cols] = df[num_cols].fillna(0)
#         df = df.fillna("")
#         data = df.to_dict(orient="records")
#         connection = create_snowflake_connection(config)
#         upload_to_snowflake(data, connection, "XRPL_ADDRESS_DETAILS")
#     else:
#         print("invalid argument")
