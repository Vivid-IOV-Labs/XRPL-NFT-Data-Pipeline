import asyncio
from datetime import datetime
import os
import json

import pandas as pd
from utilities import Config, Factory

config = Config.from_env('env/.env.local')
factory = Factory(config)

async def execute_db_query(factory: Factory, query: str, insert = False):  # noqa
    pool = await factory.get_db_client().create_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            if insert is not True:
                result = await cursor.fetchall()
            else:
                result = None
        connection.close()
    return result


df = pd.read_excel('data/accept-offers.xlsx')
to_dict = df.to_dict(orient='records')
completed = json.load(open('data/local/owner-activity/completed.json', 'r'))
for row in to_dict[:1000]:
    accept_offer_hash = row['ao_hash']
    if accept_offer_hash in completed:
        print('Already Completed. Onto the next...')
        continue
    print(f"Starting for Hash: {row['ao_hash']}")
    existing_activity_query = f"SELECT count('id') FROM nft_owner_activity WHERE accept_offer_tx_hash = '{accept_offer_hash}'"
    count_result = asyncio.run(execute_db_query(factory, existing_activity_query))
    count = count_result[0][0]
    if count > 0:
        print(f'Activity Count: {count}')
        completed[accept_offer_hash] = True
    else:
        query = f"SELECT nft_token_id, account, is_sell_offer, tx_hash FROM nft_buy_sell_offers WHERE accept_offer_hash = '{accept_offer_hash}'"
        result = asyncio.run(execute_db_query(factory, query))
        if result is None:
            print(f'Offer Not Found For {row}')
            continue
        result = result[0]
        is_sell_offer = result[2]
        offer_tx_hash = result[-1]
        nft_token_id = result[0]

        activity_timestamp = datetime.utcfromtimestamp(int(row['ao_date'] + 946684800)).strftime('%Y-%m-%d %H:%M:%S')
        if is_sell_offer:
            buyer = row['ao_account']
            seller = result[1]
        else:
            buyer = result[1]
            seller = row['ao_account']
        acquisition_query = f"INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash) VALUES ('{nft_token_id}', '{buyer}', 'Acquired', '{activity_timestamp}', '{offer_tx_hash}', '{accept_offer_hash}') RETURNING id;"
        sales_query = f"INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash) VALUES ('{nft_token_id}', '{seller}', 'Sold', '{activity_timestamp}', '{offer_tx_hash}', '{accept_offer_hash}') RETURNING id;"
        acquisition_result = asyncio.run(execute_db_query(factory, acquisition_query))
        sales_result = asyncio.run(execute_db_query(factory, sales_query))
        activity_id = str(acquisition_result[0][0])
        current_owner_query = f"INSERT INTO nft_current_owner (nft_token_id, owner_address, owner_activity) VALUES ('{nft_token_id}', '{buyer}', '{activity_id}') ON CONFLICT DO NOTHING"
        asyncio.run(execute_db_query(factory, current_owner_query, insert=True))
        completed[accept_offer_hash] = True
    json.dump(completed, open('data/local/owner-activity/completed.json', 'w'), indent=2)
    print('\n')