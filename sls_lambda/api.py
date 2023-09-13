import asyncio
import requests

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
        return f"SELECT nft_buy_sell_offers.nft_token_id, nft_buy_sell_offers.account, nft_buy_sell_offers.is_sell_offer, " \
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
            "token_id": row[0],
            "account": row[1],
            "action": str(TokenHistoryAction.CREATE_OFFER.value),
            "is_sell_offer": row[2],
            "date": self._format_date(row[5])
        }

    def _get_accept_offer_action(self, row: Tuple):
        return {
            "account": row[6],
            "date": self._format_date(row[7]),
            "action": str(TokenHistoryAction.ACCEPT_OFFER.value)
        }

    def _get_cancel_offer_action(self, row: Tuple):
        return {
            "account": row[8],
            "action": str(TokenHistoryAction.CANCEL_OFFER.value),
            "date": self._format_date(row[9])
        }

    def _get_burn_offer_action(self, row: Tuple):
        return {
            "account": row[10],
            "action": str(TokenHistoryAction.TOKEN_BURN.value),
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
        return history

