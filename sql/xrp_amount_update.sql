UPDATE nft_buy_sell_offers
SET xrp_amount = amount
WHERE xrp_amount IS NULL
    AND currency = '';