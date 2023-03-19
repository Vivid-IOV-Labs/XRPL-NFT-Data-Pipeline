from utilities import AsyncS3FileWriter, LocalFileWriter, BaseFileWriter
import pandas as pd


class Factory:

    def __init__(self, config):
        self._config = config
        self._bucket_mapping = {
            "price": config.PRICE_DUMP_BUCKET,
            "nft": config.NFT_DUMP_BUCKET,
            "csv": config.RAW_DUMP_BUCKET,
            "data": config.DATA_DUMP_BUCKET
        }
        self._supported_issuers = [
            "r3a82jDJdg4TyUMEPEH4Wpg62HniXA4Jcj", "r4zG9kcxyvq5niULmHbhUUbfh9R9nnNBJ4",
            "rfkwiAyQx884xGGWUNDDhp5DjMTtwdrQVd", "rKiNWUkVsq1rb9sWForfshDSEQDSUncwEu",
            "rLoMBprALb22SkmNK4K3hFWrpgqXbAi6qQ", "rLtgE7FjDfyJy5FGY87zoAuKtH6Bfb9QnE",
            "rUnbe8ZBmRQ7ef9EFnPd9WhXGY72GThCSc"
        ]
        self._issuers_df = pd.DataFrame()
        self._untracked_issuers = [
            "rhsxg4xH8FtYc3eR53XDSjTGfKQsaAGaqm", "rwvQWhjpUncjEbhsD2V9tv4YpKXjfH5RDj",
            "rMsZProT3MjyCHP6FD9tk4A2WrwDMc6cbE", "rJ8vugKNvcRLrxxpHzuVC2HAa7W4BcA96f",
            "r3BWpaFk3rtWhhs2Q5FwFqLVdTPnfVUJLr", "rDANq225BqjoyiFPXGcpBTzFdQTnn6aK6z",
            "rBRFkq47qJpVN4JcL13dUQaLT1HfNuBctb", "rHCRRCUEb2zJNV7FDrhzCivypExBcFT8Wy",
            "rEzbi191M5AjrucxXKZWbR5QeyfpbedBcV", "rToXSFbQ8enso9A8zbmSrxhWkaNcU6yop",
            "rBLADEZyJhCtVS1vQm3UcvLac713KyGScN", "rKEGKWH2wKCyY2GNDtkNRqXhnYEsXZM2dP",
            "ra5jrnrq9BxsvzGeJY5XS9inftcJWMdJUx", "rUCjpVXSWM4tqnG49vHPn4adm7uoz5howG",
            "rfUkZ3BVmgx5aD3Zo5bZk68hrUrhNth8y3", "rG5qYqxdmDmLkVnPrLcWKE6LYTMeFGhYy9",
            "rpbjkoncKiv1LkPWShzZksqYPzKXmUhTW7",
        ]

    def _get_bucket(self, section) -> str:
        return self._bucket_mapping[section]

    def get_writer(self, section=None) -> BaseFileWriter:
        if self._config.ENVIRONMENT == "LOCAL":
            return LocalFileWriter()
        return AsyncS3FileWriter(self._get_bucket(section))

    @property
    def config(self):
        return self._config

    @property
    def supported_issuers(self):
        if self._supported_issuers:
            return self._supported_issuers
        nft_sheet_df = pd.read_csv(self._config.NFTS_SHEET_URL)
        supported_issuers = nft_sheet_df["Issuer_Account"].values.tolist()
        self._supported_issuers = list(set(supported_issuers) - set(self._untracked_issuers))
        return self._supported_issuers

    @property
    def issuers_df(self):
        if not self._issuers_df.empty:
            return self._issuers_df
        nft_sheet_df = pd.read_csv(self._config.NFTS_SHEET_URL)
        self._issuers_df = nft_sheet_df
        return self._issuers_df
