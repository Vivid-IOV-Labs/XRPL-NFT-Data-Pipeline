import asyncio
import datetime

import httpx  # noqa
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.constants import XRPLException
from xrpl.models.requests import NFTBuyOffers, NFTSellOffers
from xrpl.models.response import ResponseStatus

from config import Config
from main import factory, logger
from utils import chunks, fetch_issuer_taxons, fetch_issuer_tokens
from writers import AsyncS3FileWriter, LocalFileWriter


class Pricing:

    async def _dump_token_offer(self, issuer, token_id, server):  # noqa
        """Fetches buy and sell offers for a particular token from the ledger"""
        now = datetime.datetime.utcnow()
        client = AsyncJsonRpcClient(server)
        buy_offer_request = NFTBuyOffers(nft_id=token_id)
        sell_offer_request = NFTSellOffers(nft_id=token_id)
        buy_offers = await client.request(buy_offer_request)
        sell_offers = await client.request(sell_offer_request)

        offers = []
        buy_offers = (
            buy_offers.result["offers"]
            if buy_offers.status == ResponseStatus.SUCCESS
            else []
        )
        sell_offers = (
            sell_offers.result["offers"]
            if sell_offers.status == ResponseStatus.SUCCESS
            else []
        )
        buy_offers_amount_arr = [
            float(offer["amount"]) for offer in buy_offers if type(offer["amount"]) != dict
        ]
        sell_offers_amount_arr = [
            float(offer["amount"]) for offer in sell_offers if type(offer["amount"]) != dict
        ]
        highest_buy_offer = max(buy_offers_amount_arr) if buy_offers_amount_arr else 0
        lowest_sell_offer = min(sell_offers_amount_arr) if sell_offers_amount_arr else 0
        offers.extend([highest_buy_offer, lowest_sell_offer])
        average = sum(offers) / len(offers) if offers else 0
        data = {
            "average": average,
            "lowest_sell": lowest_sell_offer
        }
        await AsyncS3FileWriter(Config.PRICE_DUMP_BUCKET).write_json(
            f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/tokens/{token_id}.json", data
        )


    async def _dump_taxon_token_offers(self, issuer, taxon, token_id):
        for server in Config.XRPL_SERVERS:
            try:
                await self._dump_token_offer(issuer, token_id, server)
            except (httpx.RequestError, XRPLException) as e:
                logger.error(f"Failed Using {server} with Error {e}: Retrying ...")
                continue
        logger.error(
            f"Could not get offers from any server for issuer {issuer}, taxon {taxon}, and token {token_id}"
        )


    async def _dump_taxon_offers(self, taxon, issuer, tokens):
        tokens = [token for token in tokens if token["Taxon"] == taxon]
        logger.info(f"Running for Taxon {taxon} With {len(tokens)} Tokens")
        for chunk in chunks(tokens, 50):
            await asyncio.gather(
                *[
                    self._dump_taxon_token_offers(issuer, taxon, token["NFTokenID"])
                    for token in chunk
                ]
            )
        # logger.info(f"Averages --> {averages}\n")
        # non_zero = [avg for avg in averages if avg != 0]
        # offers += non_zero
        # logger.info(f"Offers --> {offers}")
    # data = {
    #     "taxon": taxon,
    #     "issuer": issuer,
    #     "average_price": sum(offers) / len(offers) if offers else 0,
    #     "floor_price": min(offers) if offers else 0,
    # }
    # if Config.ENVIRONMENT == "LOCAL":
    #     LocalFileWriter().write_json(
    #         data,
    #         f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}",
    #         f"{taxon}.json",
    #     )
    # else:
    #     await AsyncS3FileWriter(Config.PRICE_DUMP_BUCKET).write_json(
    #         f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/{taxon}.json", data
    #     )
    # return data["floor_price"]

# async def dump_taxon_offers_aggregate(taxon, issuer, tokens)


# async def dump_issuer_taxon_offers(issuer):
#     issuers_df = factory.issuers_df
#     taxon = issuers_df[issuers_df["Issuer_Account"] == issuer].Taxon.to_list()
#
#     now = datetime.datetime.utcnow()
#     taxons = []
#     if str(taxon[0]) != "nan":
#         taxons = [int(taxon[0])]
#     else:
#         taxons = fetch_issuer_taxons(
#             issuer,
#             Config.ENVIRONMENT,
#             Config.NFT_DUMP_BUCKET,
#             Config.ACCESS_KEY_ID,
#             Config.SECRET_ACCESS_KEY,
#         )
#     tokens = fetch_issuer_tokens(
#         issuer,
#         Config.ENVIRONMENT,
#         Config.NFT_DUMP_BUCKET,
#         Config.ACCESS_KEY_ID,
#         Config.SECRET_ACCESS_KEY,
#     )
#     logger.info(f"Taxon Count: {len(taxons)}\nToken Count: {len(tokens)}")
#     # taxons = taxons[:10]  # temporary
#     average_prices = []
#     for chunk in chunks(taxons, 50):
#         prices = await asyncio.gather(
#             *[taxon_offers(taxon, issuer, tokens) for taxon in chunk]
#         )
#         average_prices.extend(prices)
#     data = {
#         "issuer": issuer,
#         "average_price": sum(average_prices) / len(average_prices)
#         if average_prices
#         else 0,
#         "floor_price": min(average_prices) if average_prices else 0,
#     }
#     if Config.ENVIRONMENT == "LOCAL":
#         LocalFileWriter().write_json(
#             data, f"data/pricing/{now.strftime('%Y-%m-%d-%H')}/{issuer}", "price.json"
#         )
#     else:
#         await AsyncS3FileWriter(Config.PRICE_DUMP_BUCKET).write_json(
#             f"{now.strftime('%Y-%m-%d-%H')}/{issuer}/price.json", data
#         )
