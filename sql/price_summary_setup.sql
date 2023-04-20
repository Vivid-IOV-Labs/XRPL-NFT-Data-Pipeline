-- Create Price Summary Table --
CREATE TABLE IF NOT EXISTS nft_pricing_summary
(
	nft_token_id text COLLATE pg_catalog."default",
	floor_price text COLLATE pg_catalog."default",
	max_buy_offer text COLLATE pg_catalog."default",
	issuer text COLLATE pg_catalog."default",
	taxon bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.nft_buy_sell_offers
    OWNER to postgres;

-- ALTER TABLE IF EXISTS nft_pricing_summary ADD UNIQUE (nft_token_id);

-- Drops the Trigger If it already exists --
DROP TRIGGER IF EXISTS price_summary_update_trigger ON nft_buy_sell_offers;

-- Creates the trigger function --
CREATE OR REPLACE FUNCTION update_token_price_summary()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE nft_pricing_summary
  SET floor_price = (
    SELECT MIN(amount) FROM nft_buy_sell_offers WHERE nft_token_id = NEW.nft_token_id AND is_sell_offer AND currency = '' AND amount::DECIMAL != 0 AND accept_offer_hash is null AND cancel_offer_hash is null
  ),
  max_buy_offer = (
    SELECT MAX(amount) FROM nft_buy_sell_offers WHERE nft_token_id = NEW.nft_token_id AND NOT is_sell_offer AND currency = '' AND accept_offer_hash is null AND cancel_offer_hash is null
  )
  WHERE nft_token_id = NEW.nft_token_id;

  IF NOT FOUND THEN
    INSERT INTO nft_pricing_summary(nft_token_id, floor_price, max_buy_offer, issuer, taxon)
    VALUES (NEW.nft_token_id, NEW.amount, NEW.amount, NEW.issuer, NEW.taxon);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Creates the Trigger --
CREATE TRIGGER price_summary_update_trigger
AFTER INSERT OR UPDATE ON nft_buy_sell_offers
FOR EACH ROW
EXECUTE FUNCTION update_token_price_summary();