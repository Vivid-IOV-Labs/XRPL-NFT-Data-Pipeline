-- Creates the trigger function --
CREATE OR REPLACE FUNCTION update_nft_sales()
RETURNS TRIGGER AS $$
DECLARE
    offer nft_buy_sell_offers%ROWTYPE;
BEGIN
    -- Check if the offer is a buy offer or sell offer
    IF NEW.buy_offer != '' THEN
        -- Fetch the corresponding offer from nft_buy_offers table
        -- based on the buy_offer value of the newly added row
        SELECT * INTO offer
        FROM nft_buy_sell_offers
        WHERE offer_index = NEW.buy_offer;
    ELSE
        -- Fetch the corresponding offer from nft_buy_offers table
        -- based on the sell_offer value of the newly added row
        SELECT * INTO offer
        FROM nft_buy_sell_offers
        WHERE offer_index = NEW.sell_offer;
    END IF;

    -- Update the sales column in the nft_sales table
	-- Update the existing record in nft_sales if it exists
	UPDATE nft_sales
	SET sales = sales + 1
	WHERE nft_token_id = offer.nft_token_id;

	-- If no rows were affected by the update, create a new record
	IF NOT FOUND THEN
	    IF offer.nft_token_id is not null THEN
            INSERT INTO nft_sales (nft_token_id, taxon, issuer, sales)
            VALUES (offer.nft_token_id, offer.taxon, offer.issuer, 1)
            ON CONFLICT DO NOTHING;
		END IF;
	END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;