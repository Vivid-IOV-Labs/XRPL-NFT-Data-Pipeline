-- Creates the trigger function --
CREATE OR REPLACE FUNCTION add_new_offer_currency()
RETURNS TRIGGER AS $$

BEGIN
    -- Check if the offer has a currency
    IF NEW.currency != '' THEN
        -- Insert the New Currency Into the Offer Currency Table
        INSERT INTO nft_offer_currency (currency, issuer, xrp_amount, last_updated)
        VALUES (NEW.currency, NEW.amount_issuer, 0.0, CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;