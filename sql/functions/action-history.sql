CREATE OR REPLACE FUNCTION create_owner_activity()
RETURNS TRIGGER AS $$
DECLARE
    offer nft_buy_sell_offers%ROWTYPE;
    activity_id UUID;
    buyer TEXT;
    seller TEXT;
BEGIN
    -- Fetch the Offer From The db --
    SELECT * INTO offer FROM nft_buy_sell_offers WHERE accept_offer_hash = NEW.hash;
    -- Insert A new Activity to the owner-activity-table if the offer exists --
    IF FOUND THEN
        -- Get The Buy & Seller Based on The Type Of Offer --
        IF offer.is_sell_offer THEN
            buyer := NEW.account;
            seller := offer.account;
        ELSE
            buyer := offer.account;
            seller := NEW.account;
        end if;
        -- Acquired token action creation --
        INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash)
        VALUES (offer.nft_token_id, buyer, 'Acquired', CURRENT_TIMESTAMP, offer.tx_hash, NEW.hash) RETURNING id INTO activity_id;
        -- Sold token action creation --
        INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash)
        VALUES (offer.nft_token_id, seller, 'Sold', CURRENT_TIMESTAMP, offer.tx_hash, NEW.hash);
        -- Update Token Owner --
        INSERT INTO nft_current_owner (nft_token_id, owner_address, owner_activity)
        VALUES (offer.nft_token_id, buyer, activity_id)
        ON CONFLICT (nft_token_id)
        DO UPDATE SET owner_address = buyer, owner_activity = EXCLUDED.owner_activity;
    end if;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;