CREATE TABLE IF NOT EXISTS nft_pricing_summary
(
	nft_token_id text COLLATE pg_catalog."default",
	floor_price text COLLATE pg_catalog."default",
	max_buy_offer text COLLATE pg_catalog."default",
	issuer text COLLATE pg_catalog."default",
	taxon bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.nft_buy_sell_offers
    OWNER to postgres;