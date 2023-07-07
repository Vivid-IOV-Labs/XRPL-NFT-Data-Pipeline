CREATE OR REPLACE FUNCTION update_burn_offer_hash()
RETURNS TRIGGER AS $$
BEGIN
    -- Update the burn_offer_hash in nft_pricing_summary if the nft_token_id exists
    UPDATE nft_pricing_summary
    SET burn_offer_hash = NEW.hash
    WHERE nft_token_id = NEW.nft_token_id;

    -- Update the burn_offer_hash in nft_volume_summary if the nft_token_id exists
    UPDATE nft_volume_summary
    SET burn_offer_hash = NEW.hash
    WHERE nft_token_id = NEW.nft_token_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;