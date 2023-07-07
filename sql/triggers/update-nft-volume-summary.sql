-- Drops The Trigger If It Exists --
DROP TRIGGER IF EXISTS update_nft_volume_summary_trigger ON nft_buy_sell_offers;
-- Creates the Trigger --
CREATE TRIGGER update_nft_volume_summary_trigger
AFTER UPDATE ON nft_buy_sell_offers
FOR EACH ROW
EXECUTE FUNCTION update_nft_volume_summary();