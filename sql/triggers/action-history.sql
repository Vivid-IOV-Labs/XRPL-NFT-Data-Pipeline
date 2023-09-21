-- Drops The Trigger If It Exists
DROP TRIGGER IF EXISTS nft_owner_action_history ON nft_accept_offer;
-- Creates the Trigger --
CREATE TRIGGER nft_owner_action_history
AFTER INSERT ON nft_accept_offer
FOR EACH ROW
EXECUTE FUNCTION create_owner_activity();