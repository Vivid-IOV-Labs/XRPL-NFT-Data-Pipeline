-- Create Price Summary Table --
CREATE TABLE IF NOT EXISTS nft_pricing_summary
(
	nft_token_id text COLLATE pg_catalog."default" UNIQUE,
	floor_price double precision COLLATE pg_catalog."default",
	max_buy_offer double precision COLLATE pg_catalog."default",
	issuer text COLLATE pg_catalog."default",
	taxon bigint,
    burn_offer_hash VARCHAR(255) UNIQUE
)

TABLESPACE pg_default;