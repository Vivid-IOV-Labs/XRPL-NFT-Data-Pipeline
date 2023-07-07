CREATE TRIGGER update_nft_sales
AFTER INSERT ON nft_accept_offer
FOR EACH ROW
EXECUTE FUNCTION update_nft_sales();