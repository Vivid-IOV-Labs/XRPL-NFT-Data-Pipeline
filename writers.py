import os.path
import aioboto3
from io import BytesIO
import json
from config import Config

import logging

logger = logging.getLogger("app_log")


class LocalFileWriter:

    def _create_path(self, path):  # noqa
        if not os.path.exists(path):
            os.makedirs(path)

    def write_json(self, data, path, file_name):
        self._create_path(path)
        json.dump(data, open(f"{path}/{file_name}", "w"), indent=4)
        logger.info(f"File Written to {path}/{file_name} Complete")


class AsyncS3FileWriter:
    def __init__(self, bucket):
        self.bucket = bucket
        self.access_key_id = Config.ACCESS_KEY_ID
        self.secret_access_key = Config.SECRET_ACCESS_KEY

    async def _write(self, path, buffer):
        # logger.info(f"Uploading File to {path}")
        session = aioboto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )
        buffer.seek(0)
        async with session.client("s3") as s3:
            await s3.upload_fileobj(buffer, self.bucket, path)
        # logger.info(f"File Uploaded to {path}")

    async def write_buffer(self, path, buffer):
        await self._write(path, buffer)

    async def write_json(self, path, obj):
        to_bytes = json.dumps(obj, indent=4).encode('utf-8')
        buffer = BytesIO()
        buffer.write(to_bytes)
        await self._write(path, buffer)

    async def write_df(self, df, path, file_type):
        buffer = BytesIO()
        if file_type == "csv":
            df.to_csv(buffer, index=False)
        await self._write(path, buffer)
