UPDATE nft_volume_summary
SET burn_offer_hash = nft_burn_offer.hash
FROM (
    SELECT nft_token_id, hash
    FROM nft_burn_offer
    ORDER BY date DESC
) AS nft_burn_offer
WHERE nft_volume_summary.nft_token_id = nft_burn_offer.nft_token_id;