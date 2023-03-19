import pytest

from utilities import LocalFileWriter, AsyncS3FileWriter


@pytest.mark.asyncio
async def test_local_file_writer(local_test_dir):
    writer = LocalFileWriter()
    await writer.write_json(f"{local_test_dir}/local-writer-test.json", {"passed": True})

@pytest.mark.asyncio
async def test_async_s3_file_writer():
    pass
