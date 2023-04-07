SELECT issuer, MIN(floor_price) AS floor_price, MAX(max_buy_offer) as max_buy_offer,
       (MAX(max_buy_offer)::DECIMAL + MIN(floor_price)::DECIMAL)/2 AS mid_price
FROM nft_pricing_summary WHERE issuer IN (
	'rfUkZ3BVmgx5aD3Zo5bZk68hrUrhNth8y3', 'rpbjkoncKiv1LkPWShzZksqYPzKXmUhTW7', 'rG5qYqxdmDmLkVnPrLcWKE6LYTMeFGhYy9',
	'rUCjpVXSWM4tqnG49vHPn4adm7uoz5howG', 'ra5jrnrq9BxsvzGeJY5XS9inftcJWMdJUx', 'rKEGKWH2wKCyY2GNDtkNRqXhnYEsXZM2dP',
	'rBLADEZyJhCtVS1vQm3UcvLac713KyGScN', 'rToXSFbQ8enso9A8zbmSrxhWkaNcU6yop', 'rEzbi191M5AjrucxXKZWbR5QeyfpbedBcV',
	'r3a82jDJdg4TyUMEPEH4Wpg62HniXA4Jcj', 'rLoMBprALb22SkmNK4K3hFWrpgqXbAi6qQ', 'rHCRRCUEb2zJNV7FDrhzCivypExBcFT8Wy',
	'rLtgE7FjDfyJy5FGY87zoAuKtH6Bfb9QnE', 'rBRFkq47qJpVN4JcL13dUQaLT1HfNuBctb', 'rDANq225BqjoyiFPXGcpBTzFdQTnn6aK6z',
	'r3BWpaFk3rtWhhs2Q5FwFqLVdTPnfVUJLr', 'r9ZW5tjbhKFLWxs4j1KqF61YSHAyDvo52D', 'rfkwiAyQx884xGGWUNDDhp5DjMTtwdrQVd',
	'rJ8vugKNvcRLrxxpHzuVC2HAa7W4BcA96f', 'rMsZProT3MjyCHP6FD9tk4A2WrwDMc6cbE', 'rMp8iuddgiseUHE94KN7G9m6jruoNK8ht7',
	'rKiNWUkVsq1rb9sWForfshDSEQDSUncwEu', 'rKmSCJzc4pbQuAxyZDskozn2mkrNNppZJE', 'rwvQWhjpUncjEbhsD2V9tv4YpKXjfH5RDj',
	'r4zG9kcxyvq5niULmHbhUUbfh9R9nnNBJ4', 'rGGQVudQM1v1tqUESWZqrEbGDvLR8XKdiY', 'rDxThQhDkAaas4Mv22DWxUsZUZE1MfvRDf',
	'rhsxg4xH8FtYc3eR53XDSjTGfKQsaAGaqm', 'rUnbe8ZBmRQ7ef9EFnPd9WhXGY72GThCSc', 'rUL4X4nLfarG9jEnsvpvoNgMcrYDaE2XHK')
GROUP BY issuer;