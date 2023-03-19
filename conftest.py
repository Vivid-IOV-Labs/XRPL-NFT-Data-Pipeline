import shutil

import pytest

@pytest.fixture
def local_test_dir():
    test_dir = 'data/test/local'
    yield test_dir
    shutil.rmtree(test_dir)