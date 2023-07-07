CREATE TRIGGER update_burn_offer_hash_trigger
AFTER INSERT ON nft_burn_offer
FOR EACH ROW
EXECUTE FUNCTION update_burn_offer_hash();