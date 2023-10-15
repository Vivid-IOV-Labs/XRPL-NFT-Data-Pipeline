CREATE OR REPLACE FUNCTION create_owner_activity()
RETURNS TRIGGER AS $$
DECLARE
    offer nft_buy_sell_offers%ROWTYPE;
    existing_activity nft_owner_activity%ROWTYPE;
    current_owner_activity nft_owner_activity%ROWTYPE;
    current_owner nft_current_owner%ROWTYPE;
    activity_id UUID;
    buyer TEXT;
    seller TEXT;
    activity_timestamp TIMESTAMP;
BEGIN
    -- Fetch the Offer From The db --
    SELECT * INTO offer FROM nft_buy_sell_offers WHERE accept_offer_hash = NEW.hash LIMIT 1;
    -- Insert A new Activity to the owner-activity-table if the offer exists --
    IF FOUND THEN
        -- Get The Buy & Seller Based on The Type Of Offer --
        activity_timestamp := to_timestamp(NEW.date + 946684800);
        IF offer.is_sell_offer THEN
            buyer := NEW.account;
            seller := offer.account;
        ELSE
            buyer := offer.account;
            seller := NEW.account;
        end if;
        -- Check if an activity has already been created for the hash and skip owner activity creation --
        SELECT * INTO existing_activity FROM nft_owner_activity WHERE accept_offer_tx_hash = NEW.hash LIMIT 1;
        IF NOT FOUND THEN
            -- Acquired token action creation --
            INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash)
            VALUES (offer.nft_token_id, buyer, 'Acquired', activity_timestamp, offer.tx_hash, NEW.hash) RETURNING id INTO activity_id;
            -- Sold token action creation --
            INSERT into nft_owner_activity (nft_token_id, owner_address, action, timestamp, create_offer_tx_hash, accept_offer_tx_hash)
            VALUES (offer.nft_token_id, seller, 'Sold', activity_timestamp, offer.tx_hash, NEW.hash);

            -- Incase of catchup/missen data update, in order to prevent changing the current owner of an nft to an old owner
            -- we need to check for existing records and compare the timestamp of the existing record to the new record before we change
            -- token ownership --
            SELECT * INTO current_owner FROM nft_current_owner WHERE nft_token_id = offer.nft_token_id;
            IF FOUND THEN
                SELECT * INTO current_owner_activity FROM nft_owner_activity WHERE id = current_owner.owner_activity;
                IF current_owner_activity.timestamp < activity_timestamp THEN
                    UPDATE nft_current_owner SET owner_address = buyer, owner_activity = activity_id WHERE nft_token_id = offer.nft_token_id;
                end if;
            ELSE
                INSERT INTO nft_current_owner (nft_token_id, owner_address, owner_activity)
                VALUES (offer.nft_token_id, buyer, activity_id);
            end if;
        end if;
    end if;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;