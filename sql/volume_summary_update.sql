INSERT INTO nft_volume_summary (nft_token_id, taxon, issuer, volume)
SELECT nft_token_id, taxon, issuer, SUM(xrp_amount::DECIMAL)
FROM nft_buy_sell_offers
WHERE accept_offer_hash IS NOT NULL
GROUP BY nft_token_id, taxon, issuer
ON CONFLICT(nft_token_id) DO UPDATE SET volume = EXCLUDED.volume