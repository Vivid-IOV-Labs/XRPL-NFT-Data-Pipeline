from config import Config
from writers import LocalFileWriter, AsyncS3FileWriter
import pandas as pd  # noqa


class Factory:

    def __init__(self):
        self._supported_issuers = None

    @staticmethod
    def get_writer(bucket=""):
        if Config.ENVIRONMENT == "LOCAL":
            return LocalFileWriter(), False
        else:
            return AsyncS3FileWriter(bucket), True

    @property
    def supported_issuers(self):
        if self._supported_issuers:
            return self.supported_issuers
        nft_sheet_df = pd.read_csv(Config.NFTS_SHEET_URL)
        supported_issuers = nft_sheet_df["Issuer_Account"].values.tolist()
        self._supported_issuers = supported_issuers
        return self._supported_issuers
