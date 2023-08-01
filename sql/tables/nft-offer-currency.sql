-- Create Offer Currency Table --
CREATE TABLE IF NOT EXISTS nft_offer_currency
(
	currency text COLLATE pg_catalog."default" UNIQUE,
	issuer text COLLATE pg_catalog."default",
	xrp_amount NUMERIC(12,2),
    last_updated TIMESTAMP
)

TABLESPACE pg_default;