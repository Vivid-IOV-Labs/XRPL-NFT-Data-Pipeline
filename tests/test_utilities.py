from utilities import Factory, BaseConfig, LocalFileWriter, AsyncS3FileWriter
from dotenv import load_dotenv


def test_config():
    # Test load config from env-file
    testing_config = BaseConfig.from_env(".env.test")
    local_config = BaseConfig.from_env(".env.local")
    prod_config = BaseConfig.from_env(".env")
    assert testing_config.ENVIRONMENT == "TESTING"
    assert local_config.ENVIRONMENT == "LOCAL"
    assert prod_config.ENVIRONMENT == "PROD"

    # Test Load config without env-file
    load_dotenv(".env", override=True)
    config = BaseConfig.from_env()
    assert config.ENVIRONMENT == "PROD"

def test_factory():
    testing_config = BaseConfig.from_env(".env.test")
    testing_factory = Factory(testing_config)

    local_writer = testing_factory.get_writer()
    db_client = testing_factory.get_db_client()
    assert testing_factory.config.ENVIRONMENT == "TESTING"
    assert local_writer.__class__ == LocalFileWriter
    assert db_client.config == testing_config

    # Test Write Proxy DB Client
    testing_config.WRITE_PROXY = "proxy.aws.com"
    db_client = testing_factory.get_db_client(write_proxy=True)
    assert db_client.config.PROXY_CONN_INFO["host"] == testing_config.WRITE_PROXY

def test_db_client():
    pass

def test_trigger_manager():
    pass

def test_writers():
    pass

def test_twitter():
    pass