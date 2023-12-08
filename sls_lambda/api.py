import asyncio
from datetime import datetime
import base64
import json

from .base import BaseLambdaRunner
from typing import Tuple
from enum import Enum


class TokenHistoryAction(Enum):
    CREATE_OFFER = "CREATE_OFFER"
    ACCEPT_OFFER = "ACCEPT_OFFER"
    CANCEL_OFFER = "CANCEL_OFFER",
    TOKEN_BURN = "TOKEN_BURN"



class TokenHistoryFetcher(BaseLambdaRunner):
    def run(self) -> None:
        pass

    @staticmethod
    def _get_query(token_id: str):
        return f"SELECT DISTINCT nft_buy_sell_offers.nft_token_id, nft_buy_sell_offers.account, nft_buy_sell_offers.is_sell_offer, " \
               f"nft_buy_sell_offers.accept_offer_hash, nft_buy_sell_offers.cancel_offer_hash, nft_buy_sell_offers.date as " \
               f"offer_date, nft_accept_offer.account " \
               f"as accept_offer_account, nft_accept_offer.date as accept_offer_date, nft_cancel_offer.account as " \
               f"cancel_offer_account, nft_cancel_offer.date as cancel_offer_date, nft_burn_offer.account as " \
               f"burn_offer_account, nft_burn_offer.date as burn_offer_date FROM nft_buy_sell_offers FULL OUTER JOIN " \
               f"nft_accept_offer ON nft_buy_sell_offers.accept_offer_hash = nft_accept_offer.hash FULL OUTER JOIN " \
               f"nft_cancel_offer ON nft_buy_sell_offers.cancel_offer_hash = nft_cancel_offer.hash FULL OUTER JOIN " \
               f"nft_burn_offer ON nft_buy_sell_offers.nft_token_id = nft_burn_offer.nft_token_id WHERE " \
               f"nft_buy_sell_offers.nft_token_id = '{token_id}'"

    async def perform_db_query(self, nft_token_id: str):
        db_client = self.factory.get_db_client()
        pool = await db_client.create_db_pool()
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = self._get_query(nft_token_id)
                await cursor.execute(query)
                result = await cursor.fetchall()
            connection.close()
        return result

    @staticmethod
    def _format_date(date: int):
        if date is None:
            return
        return int(date) + 946684800

    def _get_create_offer_action(self, row: Tuple):
        return {
            "account": row[1],
            "action": "CREATE_OFFER",
            "is_sell_offer": row[2],
            "date": self._format_date(row[5])
        }

    def _get_accept_offer_action(self, row: Tuple):
        return {
            "account": row[6],
            "date": self._format_date(row[7]),
            "action": "ACCEPT_OFFER"
        }

    def _get_cancel_offer_action(self, row: Tuple):
        return {
            "account": row[8],
            "action": "CANCEL_OFFER",
            "date": self._format_date(row[9])
        }

    def _get_burn_offer_action(self, row: Tuple):
        return {
            "account": row[10],
            "action": "TOKEN_BURN",
            "date": self._format_date(row[11])
        }

    def fetch_history(self, token_id: str):
        result = asyncio.run(self.perform_db_query(token_id))
        history = []
        for row in result:
            create_offer_action = self._get_create_offer_action(row)
            accept_offer_action = self._get_accept_offer_action(row)
            cancel_offer_action = self._get_cancel_offer_action(row)
            burn_offer_action = self._get_burn_offer_action(row)
            if create_offer_action.get("account") is not None:
                history.append(create_offer_action)
            if accept_offer_action.get("account") is not None:
                history.append(accept_offer_action)
            if cancel_offer_action.get("account") is not None:
                history.append(cancel_offer_action)
            if burn_offer_action.get("account") is not None:
                history.append(burn_offer_action)
        history = sorted(history, key=lambda action: action['date'], reverse=True)
        return {
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Methods": "GET, OPTIONS, HEAD",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token"
            },
            "statusCode": 200,
            "body": base64.b64encode(bytes(json.dumps(history), "utf-8")).decode("utf-8"),
            "isBase64Encoded": True
        }


class AccountActivity(BaseLambdaRunner):
    per_page = 10
    def run(self) -> None:
        pass

    async def _get_account_activity(self, address: str, offset: int):
        db_client = self.factory.get_db_client()
        pool = await db_client.create_db_pool()  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = (f"SELECT DISTINCT nft_token_id, action, timestamp FROM nft_owner_activity WHERE owner_address = "
                         f"'{address}' ORDER BY timestamp DESC LIMIT {self.per_page} OFFSET {offset}")
                await cursor.execute(query)
                result = await cursor.fetchall()
            connection.close()
        return result

    def fetch_activity(self, address: str, offset: int):
        """
        :param address: XRPL Address
        :param offset: DB Query Limit for pagination
        :return: history of nft sales and acquisition
        """
        nft_activity = asyncio.run(self._get_account_activity(address, offset))
        data = [{
            'account': address,
            'token_id': activity[0],
            'action': activity[1],
            'timestamp': int(activity[2].timestamp())
        } for activity in nft_activity]
        return {
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Methods": "GET, OPTIONS, HEAD",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token"
            },
            "statusCode": 200,
            "body": base64.b64encode(bytes(json.dumps(data), "utf-8")).decode("utf-8"),
            "isBase64Encoded": True
        }


class AccountNFTS(BaseLambdaRunner):
    per_page = 10
    def run(self) -> None:
        pass

    async def _get_account_nfts(self, address: str, offset: int):
        db_client = self.factory.get_db_client()
        pool = await db_client.create_db_pool()  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = (
                    f"SELECT nft_current_owner.nft_token_id, nft_owner_activity.timestamp FROM nft_current_owner JOIN nft_owner_activity ON nft_current_owner.owner_activity = nft_owner_activity.id WHERE nft_current_owner.owner_address = '{address}'"
                    f"LIMIT {self.per_page} OFFSET {offset}")
                await cursor.execute(query)
                result = await cursor.fetchall()
            connection.close()
        return result

    def fetch(self, address: str, offset: int):
        nfts = asyncio.run(self._get_account_nfts(address, offset))
        data = [{'token_id': data[0], 'acquired_at': int(data[1].timestamp())} for data in nfts]
        return {
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Methods": "GET, OPTIONS, HEAD",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token"
            },
            "statusCode": 200,
            "body": base64.b64encode(bytes(json.dumps(data), "utf-8")).decode("utf-8"),
            "isBase64Encoded": True
        }
