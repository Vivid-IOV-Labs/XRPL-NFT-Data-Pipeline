from abc import ABCMeta, abstractmethod
from utilities import factory

class BaseLambdaRunner(metaclass=ABCMeta):
    def __init__(self):
        self.issuers_df = factory.issuers_df
        self.supported_issuers = factory.supported_issuers
        self.environment = factory.config.ENVIRONMENT

    def _set_writer(self, section):
        self.writer = factory.get_writer(section)

    @abstractmethod
    async def run(self) -> None:
        raise NotImplementedError
