import asyncio
import sys

from utilities import factory, TriggerManager


async def nft_pricing_summary(action: str):
    manager = TriggerManager(
        factory=factory,
        trigger_create_script="sql/triggers/price-summary-update.sql",
        function_script="sql/functions/update-token-price-summary.sql",
        table_create_script="sql/tables/nft-price-summary.sql",
        allow_table_create=True
    )

    if action == "create":
        await manager.create_trigger()
    elif action == "update-function":
        await manager.update_function()
    else:
        print("Invalid Action. Supported Actions are `create`, `update-function`")


async def nft_volume_summary(action: str):
    manager = TriggerManager(
        factory=factory,
        trigger_create_script="sql/triggers/update-nft-volume-summary.sql",
        function_script="sql/functions/update-nft-volume-summary.sql",
        table_create_script="sql/tables/nft-volume-summary.sql",
        allow_table_create=True
    )

    if action == "create":
        await manager.create_trigger()
    elif action == "update-function":
        await manager.update_function()
    else:
        print("Invalid Action. Supported Actions are `create`, `update-function`")


async def nft_burn_offer(action: str):
    manager = TriggerManager(
        factory=factory,
        trigger_create_script="sql/triggers/update-burn-offer-hash.sql",
        function_script="sql/functions/update-burn-offer-hash.sql",
    )

    if action == "create":
        await manager.create_trigger()
    elif action == "update-function":
        await manager.update_function()
    else:
        print("Invalid Action. Supported Actions are `create`, `update-function`")


async def nft_sales(action: str):
    manager = TriggerManager(
        factory=factory,
        trigger_create_script="sql/triggers/update-nft-sales.sql",
        function_script="sql/functions/update-nft-sales.sql",
        table_create_script="sql/tables/nft-sales.sql",
        allow_table_create=True
    )

    if action == "create":
        await manager.create_trigger()
    elif action == "update-function":
        await manager.update_function()
    else:
        print("Invalid Action. Supported Actions are `create`, `update-function`")
# async def xrp_amount_update():
#     db_client = factory.get_db_client()
#     db_client.config.PROXY_CONN_INFO[
#         "host"
#     ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"
#
#     pool = await db_client.create_db_pool()
#     async with pool.acquire() as connection:
#         await execute_sql_file(connection, "sql/xrp_amount_update.sql")
#         connection.close()



# async def pricing_summary_burn_offer_update():
#     db_client = factory.get_db_client()
#     db_client.config.PROXY_CONN_INFO[
#         "host"
#     ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"
#
#     pool = await db_client.create_db_pool()
#     async with pool.acquire() as connection:
#         await execute_sql_file(connection, "sql/update_price_summary_burn_offer.sql")
#         connection.close()
#
# async def volume_summary_burn_offer_update():
#     db_client = factory.get_db_client()
#     db_client.config.PROXY_CONN_INFO[
#         "host"
#     ] = "xrpl-production-datastore.cluster-cqq7smgnm9yf.eu-west-2.rds.amazonaws.com"
#
#     pool = await db_client.create_db_pool()
#     async with pool.acquire() as connection:
#         # await execute_sql_file(connection, "sql/volume_summary_table_update.sql")
#         # await execute_sql_file(connection, "sql/update_volume_summary_burn_offer_hash.sql")
#         await execute_sql_file(connection, "sql/burn_offer_trigger.sql")
#         connection.close()

if __name__ == "__main__":
    args = sys.argv
    if args[1] == "price-summary":
        asyncio.run(nft_pricing_summary("create"))
    elif args[1] == "volume-summary":
        asyncio.run(nft_volume_summary("create"))
    # elif args[1] == "amount-update":
    #     asyncio.run(xrp_amount_update())
    elif args[1] == "nft-burn-offer":
        asyncio.run(nft_burn_offer("create"))
    elif args[1] == "nft-sales":
        asyncio.run(nft_sales("create"))
    # elif args[1] == "price-summary-burn-offer-update":
    #     asyncio.run(pricing_summary_burn_offer_update())
    # elif args[1] == "volume-summary-burn-offer-update":
    #     asyncio.run(volume_summary_burn_offer_update())
