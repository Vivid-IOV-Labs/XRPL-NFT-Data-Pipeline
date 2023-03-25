import shutil

import pytest

@pytest.fixture(scope="session")
def fs_fixture():
    test_dir = 'data/test/local'
    yield {"local": test_dir, "s3": ""}
    shutil.rmtree(test_dir)