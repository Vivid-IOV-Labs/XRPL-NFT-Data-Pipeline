CREATE TYPE ActionType AS ENUM ('Sold', 'Acquired');

CREATE TABLE nft_owner_activity (
    id UUID DEFAULT uuid_generate_v4() UNIQUE,
    nft_token_id TEXT NOT NULL,
    owner_address TEXT NOT NULL,
    action ActionType NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    create_offer_tx_hash TEXT,
    accept_offer_tx_hash TEXT
);