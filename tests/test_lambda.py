import time

import pytest

from sls_lambda import CSVDump

def test_csv_dump(setup):
    factory = setup["factory"]
    runner = CSVDump(factory)
    runner.run()
