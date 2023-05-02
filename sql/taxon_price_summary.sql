SELECT issuer,
       taxon,
       MIN(CASE WHEN is_sell_offer = TRUE AND xrp_amount::DECIMAL != 0 THEN xrp_amount END) AS floor_price,
       MAX(CASE WHEN is_sell_offer = FALSE THEN xrp_amount END) AS max_buy_offer
FROM nft_buy_sell_offers
WHERE accept_offer_hash is null AND cancel_offer_hash is null
GROUP BY issuer, taxon