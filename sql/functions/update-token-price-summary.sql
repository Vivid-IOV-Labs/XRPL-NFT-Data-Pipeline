-- Creates the trigger function --
CREATE OR REPLACE FUNCTION update_token_price_summary()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE nft_pricing_summary
  SET floor_price = (
    SELECT MIN(xrp_amount) FROM nft_buy_sell_offers WHERE nft_token_id = NEW.nft_token_id AND is_sell_offer AND xrp_amount::DECIMAL != 0 AND accept_offer_hash is null AND cancel_offer_hash is null
  ),
  max_buy_offer = (
    SELECT MAX(xrp_amount) FROM nft_buy_sell_offers WHERE nft_token_id = NEW.nft_token_id AND NOT is_sell_offer AND accept_offer_hash is null AND cancel_offer_hash is null
  )
  WHERE nft_token_id = NEW.nft_token_id;

  IF NOT FOUND THEN
    INSERT INTO nft_pricing_summary(nft_token_id, floor_price, max_buy_offer, issuer, taxon)
    VALUES (NEW.nft_token_id, NEW.amount, NEW.amount, NEW.issuer, NEW.taxon);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;