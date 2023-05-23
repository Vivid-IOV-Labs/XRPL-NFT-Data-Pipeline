import json
import logging
import os.path
from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import Dict, Union, List

import aioboto3

from .config import Config

logger = logging.getLogger("app_log")


class BaseFileWriter(metaclass=ABCMeta):
    @abstractmethod
    async def _write(self, path: str, buffer: BytesIO) -> None:
        raise NotImplementedError

    async def write_json(self, path: str, data: Union[Dict, List]) -> None:
        to_bytes = json.dumps(data, indent=4).encode("utf-8")
        buffer = BytesIO()
        buffer.write(to_bytes)
        await self._write(path, buffer)

    async def write_df(self, df, path, file_type):
        buffer = BytesIO()
        if file_type == "csv":
            df.to_csv(buffer, index=False)
        await self._write(path, buffer)

    async def write_buffer(self, path, buffer):
        await self._write(path, buffer)


class LocalFileWriter(BaseFileWriter):

    def __init__(self, testing=False):
        if testing:
            self.test_dir = "test"
        else:
            self.test_dir = None

    def _create_path(self, path):  # noqa
        if not os.path.exists(path):
            os.makedirs(path)

    async def _write(self, path, buffer):
        path = ""
        if self.test_dir is not None:
            path = f"data/testing/{path}"
        else:
            path = f"data/local/{path}"
        directory_path = "/".join(path.split("/")[:-1])
        self._create_path(directory_path)
        content = buffer.getvalue()
        with open(path, "wb") as file:
            file.write(content)
        logger.info(f"File writen to {path}")


class AsyncS3FileWriter(BaseFileWriter):
    def __init__(self, bucket):
        self.bucket = bucket
        self.access_key_id = Config.ACCESS_KEY_ID
        self.secret_access_key = Config.SECRET_ACCESS_KEY

    async def _write(self, path, buffer):
        session = aioboto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )
        buffer.seek(0)
        async with session.client("s3") as s3:
            await s3.upload_fileobj(buffer, self.bucket, path)
        logger.info(f"File writen to {path} in bucket {self.bucket}")

    async def write_json(self, path, obj):
        to_bytes = json.dumps(obj, indent=4).encode("utf-8")
        buffer = BytesIO()
        buffer.write(to_bytes)
        await self._write(path, buffer)
