from abc import ABCMeta, abstractmethod

from utilities import Factory


class BaseLambdaRunner(metaclass=ABCMeta):
    def __init__(self, factory: Factory):
        self.factory = factory
        self.db_client = factory.get_db_client()

    def _set_writer(self, section):
        self.writer = self.factory.get_writer(section)

    @staticmethod
    def _get_response_headers():
        return {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
            "Access-Control-Allow-Methods": "GET, OPTIONS, HEAD",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token"
        }

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
