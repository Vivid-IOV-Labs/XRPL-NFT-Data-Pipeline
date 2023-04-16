SELECT issuer,
       taxon,
       MIN(CASE WHEN is_sell_offer = TRUE AND amount::DECIMAL != 0 THEN amount END) AS floor_price,
       MAX(CASE WHEN is_sell_offer = FALSE THEN amount END) AS max_buy_offer
FROM nft_buy_sell_offers
WHERE currency = ''
GROUP BY issuer, taxon