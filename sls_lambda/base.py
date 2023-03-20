from abc import ABCMeta, abstractmethod
from utilities import Factory

class BaseLambdaRunner(metaclass=ABCMeta):
    def __init__(self, factory: Factory):
        self.factory = factory
        self.db_client = factory.get_db_client()
        self.issuers_df = factory.issuers_df
        self.supported_issuers = factory.supported_issuers
        self.environment = factory.config.ENVIRONMENT
        self.config = factory.config

    def _set_writer(self, section):
        self.writer = self.factory.get_writer(section)

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
