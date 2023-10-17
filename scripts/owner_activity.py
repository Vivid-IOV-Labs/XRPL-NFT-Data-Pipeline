import asyncio
from datetime import datetime, timezone
import time
import json

from utilities import Factory


async def execute_db_query(factory: Factory, query: str, fetch_result = True):  # noqa
    pool = await factory.get_db_client().create_db_pool()
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            if fetch_result:
                result = await cursor.fetchall()
            else:
                result = None
        connection.close()
    return result

def owner_activity_catchup(factory: Factory):
    # Fetch All Accept offers {Fetch by batch}
    start = time.monotonic()
    completed = json.load(open('data/local/owner-activity/completed.json', 'r'))
    batch_size = 1000
    current_batch = completed.get('last_completed_batch', 1)
    while True:
        print('\n')
        print(f'Running For Batch {current_batch}')
        accept_offers_query = f'SELECT hash, date, account FROM nft_accept_offer LIMIT {batch_size} offset {(current_batch - 1)*batch_size}'
        accept_offers = asyncio.run(execute_db_query(factory, accept_offers_query))
        if len(accept_offers) == 0:
            break
        for row in accept_offers:
            accept_offer_hash = row[0]
            accept_offer_date = row[1]
            accept_offer_account = row[2]
            activity_timestamp = datetime.utcfromtimestamp(int(int(accept_offer_date) + 946684800)).strftime(
                '%Y-%m-%d %H:%M:%S')
            if accept_offer_hash in completed:
                print('Already Completed. Onto the next...')
                continue
            print(f"Starting for Hash: {accept_offer_hash}")
            existing_activity_query = f"SELECT count('id') FROM nft_owner_activity WHERE accept_offer_tx_hash = '{accept_offer_hash}'"
            count_result = asyncio.run(execute_db_query(factory, existing_activity_query))
            count = count_result[0][0]
            if count > 0:
                print(f'Activity Count: {count} With Hash: {accept_offer_hash}')
                # Update the activity with the right date
                update_query = f"UPDATE nft_owner_activity SET timestamp = '{activity_timestamp}' WHERE accept_offer_tx_hash = '{accept_offer_hash}'"
                asyncio.run(execute_db_query(factory, update_query, fetch_result=False))
                completed[accept_offer_hash] = True
            else:
                offer_query = f"SELECT nft_token_id, account, is_sell_offer, tx_hash FROM nft_buy_sell_offers WHERE accept_offer_hash = '{accept_offer_hash}'"
                result = asyncio.run(execute_db_query(factory, offer_query))
                if result is None or result == []:
                    print(f'Offer Not Found For Hash: {accept_offer_hash}')
                    continue
                result = result[0]
                is_sell_offer = result[2]
                offer_tx_hash = result[-1]
                nft_token_id = result[0]

                if is_sell_offer:
                    buyer = accept_offer_account
                    seller = result[1]
                else:
                    buyer = result[1]
                    seller = accept_offer_account
                acquisition_query = f"INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash) VALUES ('{nft_token_id}', '{buyer}', 'Acquired', '{activity_timestamp}', '{offer_tx_hash}', '{accept_offer_hash}') RETURNING id;"
                sales_query = f"INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash) VALUES ('{nft_token_id}', '{seller}', 'Sold', '{activity_timestamp}', '{offer_tx_hash}', '{accept_offer_hash}') RETURNING id;"
                acquisition_result = asyncio.run(execute_db_query(factory, acquisition_query))
                asyncio.run(execute_db_query(factory, sales_query))
                buy_activity_id = str(acquisition_result[0][0])
                current_owner_query = f"SELECT nft_owner_activity.timestamp FROM nft_current_owner JOIN nft_owner_activity ON nft_current_owner.owner_activity = nft_owner_activity.id WHERE nft_current_owner.nft_token_id = '{nft_token_id}'"
                existing_current_owner = asyncio.run(execute_db_query(factory, current_owner_query))
                if existing_current_owner is None or existing_current_owner == []:
                    current_owner_insert_query = f"INSERT INTO nft_current_owner (nft_token_id, owner_address, owner_activity) VALUES ('{nft_token_id}', '{buyer}', '{buy_activity_id}')"
                    asyncio.run(execute_db_query(factory, current_owner_insert_query, fetch_result=False))
                else:
                    # check the date of the activity and update accordingly
                    existing_activity_ts = existing_current_owner[0][0].replace(tzinfo=timezone.utc).timestamp()
                    new_activity_ts = datetime.utcfromtimestamp(int(int(accept_offer_date) + 946684800)).timestamp()
                    if new_activity_ts > existing_activity_ts:
                        # Update the current owner and the activity
                        update_query = f"UPDATE nft_current_owner SET owner_address = '{buyer}', owner_activity = '{buy_activity_id}' WHERE nft_token_id = '{nft_token_id}'"
                        asyncio.run(execute_db_query(factory, update_query, fetch_result=False))
                completed[accept_offer_hash] = True
            completed['last_completed_batch'] = current_batch
            json.dump(completed, open('data/local/owner-activity/completed.json', 'w'), indent=2)
        current_batch += 1
    end = time.monotonic()
    print(f'Executed in {end - start} secs')
