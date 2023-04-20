CREATE TABLE IF NOT EXISTS public.nft_buy_sell_offers
(
    offer_index text COLLATE pg_catalog."default",
    tx_hash text COLLATE pg_catalog."default",
    account text COLLATE pg_catalog."default",
    destination text COLLATE pg_catalog."default",
    fee text COLLATE pg_catalog."default",
    nft_token_id text COLLATE pg_catalog."default",
    amount text COLLATE pg_catalog."default",
    currency text COLLATE pg_catalog."default",
    amount_issuer text COLLATE pg_catalog."default",
    sign_public_key text COLLATE pg_catalog."default",
    date text COLLATE pg_catalog."default",
    tx_sign text COLLATE pg_catalog."default",
    issuer text COLLATE pg_catalog."default",
    owner text COLLATE pg_catalog."default",
    uri text COLLATE pg_catalog."default",
    meta jsonb,
    memos text COLLATE pg_catalog."default",
    is_sell_offer boolean,
    sequence bigint,
    ledger_index bigint,
    taxon bigint,
    transfer_fee bigint,
    flags bigint,
    last_ledger_sequence bigint,
    signers jsonb,
    accept_offer_hash text COLLATE pg_catalog."default",
    cancel_offer_hash text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.nft_buy_sell_offers
    OWNER to postgres;

CREATE TABLE IF NOT EXISTS public.nft_pricing_summary
(
    nft_token_id text COLLATE pg_catalog."default",
    floor_price text COLLATE pg_catalog."default",
    max_buy_offer text COLLATE pg_catalog."default",
    issuer text COLLATE pg_catalog."default",
    taxon bigint,
    CONSTRAINT nft_pricing_summary_nft_token_id_key UNIQUE (nft_token_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.nft_pricing_summary
    OWNER to postgres;