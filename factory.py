from config import Config
from writers import LocalFileWriter, AsyncS3FileWriter


class Factory:

    @staticmethod
    def get_writer(bucket=""):
        if Config.ENVIRONMENT == "LOCAL":
            return LocalFileWriter(), False
        else:
            return AsyncS3FileWriter(bucket), True