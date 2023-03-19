import pytest

from utilities import LocalFileWriter, AsyncS3FileWriter


@pytest.mark.asyncio
async def test_local_file_writer(fs_fixture):
    local_test_dir = fs_fixture["local"]
    writer = LocalFileWriter()
    await writer.write_json(f"{local_test_dir}/local-writer-test.json", {"passed": True})

@pytest.mark.asyncio
async def test_async_s3_file_writer(fs_fixture):
    pass
