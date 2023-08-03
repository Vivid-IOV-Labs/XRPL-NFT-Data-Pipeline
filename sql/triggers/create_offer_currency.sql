-- Drops The Trigger If It Exists
DROP TRIGGER IF EXISTS create_offer_currency ON nft_buy_sell_offers;
-- Creates the Trigger --
CREATE TRIGGER create_offer_currency
AFTER INSERT OR UPDATE ON nft_buy_sell_offers
FOR EACH ROW
EXECUTE FUNCTION add_new_offer_currency();