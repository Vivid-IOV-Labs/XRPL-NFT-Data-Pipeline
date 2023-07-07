-- Create Price Summary Table --
CREATE TABLE IF NOT EXISTS nft_volume_summary
(
	nft_token_id text COLLATE pg_catalog."default" UNIQUE,
	burn_offer_hash VARCHAR(255) UNIQUE,
	issuer text COLLATE pg_catalog."default",
	taxon bigint,
	volume bigint
)

TABLESPACE pg_default;