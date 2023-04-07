INSERT INTO nft_pricing_summary (nft_token_id, issuer, taxon, floor_price, max_buy_offer)
SELECT nft_token_id,
       issuer,
       taxon,
       MIN(CASE WHEN is_sell_offer = 1 THEN amount END) AS floor_price,
       MAX(CASE WHEN is_sell_offer = 0 THEN amount END) AS max_buy_offer
FROM nft_buy_sell_offers
WHERE currency = ''
GROUP BY nft_token_id
ON DUPLICATE KEY UPDATE
    floor_price = VALUES(floor_price),
    max_buy_offer = VALUES(max_buy_offer);
