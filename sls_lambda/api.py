import asyncio
from datetime import datetime

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


class TokenOwnershipHistory(BaseLambdaRunner):
    per_page = 10
    def run(self) -> None:
        pass

    async def _get_account_accept_offers(self, address: str, offset: int):
        db_client = self.factory.get_db_client()
        pool = await db_client.create_db_pool()  # noqa
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = (
                    f"SELECT nft_accept_offer.account, nft_accept_offer.date, nft_accept_offer.hash, nft_accept_offer."
                    f"sell_offer, nft_accept_offer.buy_offer, nft_buy_sell_offers.nft_token_id, nft_buy_sell_offers.account as offer_creator, "
                    f"nft_buy_sell_offers.is_sell_offer FROM nft_accept_offer "
                    f"FULL OUTER JOIN nft_buy_sell_offers ON nft_buy_sell_offers.accept_offer_hash = nft_accept_offer."
                    f"hash WHERE nft_token_id is not NULL AND nft_accept_offer.account = "
                    f"'{address}' LIMIT {self.per_page} OFFSET {offset}")
                await cursor.execute(query)
                result = await cursor.fetchall()
            connection.close()
        return sorted([{
            'account': offer[0],
            'timestamp': int(offer[1]) + 946684800,
            'hash': offer[2],
            'token_id': offer[5],
            'created_by': offer[6],
            'is_sell_offer': offer[7]
        } for offer in result], key=lambda offer: offer['timestamp'])

    def fetch_history(self, address: str, offset: int):
        """
        WIP

        This method gets the history of token held by an address. The period the token was held for and who the new owner is.
        There are various scenarios to take into account which might lead to ownership or transfer of tokens.
        1. Accepting a sell offer created by another token owner transfers the token to the accepting address(the address arg)
        2. Accepting a buy offer created by another account would indicate end of ownership of such tokens. Transfers the token to the address
           that created the buy offer.
        3. If a token owner creates a sell offer and the sell offer gets accepted by another account, it also indicates transfer/end of ownership of such token.
        4. If the account of interest(argument) creates a buy offer for another token and the buy offer gets accepted, this indicates beginning of ownership.

        This function looks to cover these 4 possible scenarios(open to extension in the future) in the most efficient way.
        :param address: XRPL Address
        :param offset: DB Query Limit for pagination
        :return: history of current and past nfts held by an address
        """
        accept_offers = asyncio.run(self._get_account_accept_offers(address, offset))
        history = {}
        for offer in accept_offers:
            token_id = offer['token_id']
            accepted_at = datetime.fromtimestamp(offer['timestamp'])
            created_by = offer['created_by']
            if offer['is_sell_offer']:
                # Accepted A sell offer [ Becomes the token owner ]
                history[token_id] = {
                    'hold_start': int(accepted_at.timestamp()),
                    'hold_end': None,
                    'previous_owner': created_by,
                    'new_owner': address
                }
                # print(f"Bought token {token_id} at {accepted_at} from {created_by}")
            else:
                # Accepts A Buy Offer [ Sells token to another account ]
                if token_id in history:
                    history[token_id]['hold_end'] = int(accepted_at.timestamp())  # noqa
                    history[token_id]['previous_owner'] = address
                    history[token_id]['new_owner'] = created_by
                    # print(f"Sold token {token_id} to {created_by} at {accepted_at}")
        return [{'token_id': token_id, **history[token_id]} for token_id in history]
        #return history
