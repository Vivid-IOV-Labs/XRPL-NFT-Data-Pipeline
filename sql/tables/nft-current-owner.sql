CREATE TABLE nft_current_owner (
	nft_token_id TEXT UNIQUE NOT NULL,
    owner_address TEXT NOT NULL,
    owner_activity UUID,
    FOREIGN KEY (owner_activity) REFERENCES nft_owner_activity(id)
);