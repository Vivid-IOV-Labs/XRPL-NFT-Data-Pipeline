INSERT INTO nft_buy_sell_offers (tx_hash, account, is_sell_offer, nft_token_id, accept_offer_hash) VALUES ('off-hash-1', 'account1', true, 'token-x', 'accept1');
INSERT INTO nft_buy_sell_offers (tx_hash, account, is_sell_offer, nft_token_id, accept_offer_hash) VALUES ('off-hash-2', 'account2', true, 'token-y', 'accept2');
INSERT INTO nft_accept_offer(account, buy_offer, hash) VALUES ('account2', 'blabla', 'accept1');
INSERT INTO nft_accept_offer(account, buy_offer, hash) VALUES ('account1', 'blabl', 'accept2');