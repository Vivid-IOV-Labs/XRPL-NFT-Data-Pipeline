-- Creates the trigger function --
CREATE OR REPLACE FUNCTION update_nft_volume_summary()
RETURNS TRIGGER AS $$
BEGIN
  IF (OLD.accept_offer_hash IS DISTINCT FROM NEW.accept_offer_hash) THEN
    -- Retrieve the nft_token_id, taxon, issuer, and amount values from the updated row
    DECLARE
      nft_token_id_value TEXT := NEW.nft_token_id;
      taxon_value NUMERIC := NEW.taxon;
      issuer_value TEXT := NEW.issuer;
      amount_value NUMERIC := NEW.xrp_amount;
    BEGIN
      -- Insert or update the nft_volume_summary table with the nft_token_id, taxon, issuer, and amount values
      INSERT INTO nft_volume_summary (nft_token_id, taxon, issuer, volume)
        VALUES (nft_token_id_value, taxon_value, issuer_value, amount_value)
        ON CONFLICT (nft_token_id)
        DO UPDATE SET volume = nft_volume_summary.volume + EXCLUDED.volume;
    END;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;