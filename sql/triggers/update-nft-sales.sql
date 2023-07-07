-- Drops the Trigger If it already exists --
DROP TRIGGER IF EXISTS update_nft_sales ON nft_accept_offer;
-- Creates The Trigger --
CREATE TRIGGER update_nft_sales
AFTER INSERT ON nft_accept_offer
FOR EACH ROW
EXECUTE FUNCTION update_nft_sales();