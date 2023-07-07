-- Create Sales Table --
CREATE TABLE IF NOT EXISTS nft_sales
(
	nft_token_id text COLLATE pg_catalog."default" UNIQUE,
	issuer text COLLATE pg_catalog."default",
	taxon bigint,
	sales bigint
)

TABLESPACE pg_default;