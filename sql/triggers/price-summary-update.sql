-- Drops The Trigger If It Exists
DROP TRIGGER IF EXISTS price_summary_update_trigger ON nft_buy_sell_offers;
-- Creates the Trigger --
CREATE TRIGGER price_summary_update_trigger
AFTER INSERT OR UPDATE ON nft_buy_sell_offers
FOR EACH ROW
EXECUTE FUNCTION update_token_price_summary();