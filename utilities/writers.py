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
        elif file_type == "json":
            df.to_json(buffer, indent=4, orient="records")
        await self._write(path, buffer)

    async def write_buffer(self, path, buffer):
        await self._write(path, buffer)


class LocalFileWriter(BaseFileWriter):

    def __init__(self, testing=False):
        if testing:
            self.base_dir = "data/test"
        else:
            self.base_dir = "data/local"
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def _create_path(self, path):  # noqa
        if not os.path.exists(path):
            os.makedirs(path)

    def _get_dir_from_file_path(self, file_path):  # noqa
        return "/".join(file_path.split("/")[:-1])


    async def _write(self, path, buffer):
        file_path = f"{self.base_dir}/{path}"
        file_dir = self._get_dir_from_file_path(file_path)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        content = buffer.getvalue()
        with open(file_path, "wb") as file:
            file.write(content)
        logger.info(f"File writen to {file_path}")


class AsyncS3FileWriter(BaseFileWriter):
    def __init__(self, bucket, config: Config):
        self.bucket = bucket
        self.access_key_id = config.ACCESS_KEY_ID
        self.secret_access_key = config.SECRET_ACCESS_KEY

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
