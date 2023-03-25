from abc import ABCMeta, abstractmethod

from utilities import Factory


class BaseLambdaRunner(metaclass=ABCMeta):
    def __init__(self, factory: Factory):
        self.factory = factory
        self.db_client = factory.get_db_client()

    def _set_writer(self, section):
        self.writer = self.factory.get_writer(section)

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
