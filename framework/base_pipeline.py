from abc import ABC, abstractmethod

from framework.logger import LoggerManager
from framework.spark import SparkManager


class BasePipeline(ABC):

    def __init__(self):
        self.logger = LoggerManager().get_logger()
        self.spark = SparkManager().get_session()

    @abstractmethod
    def extract(self): ...

    @abstractmethod
    def transform(self, df): ...

    @abstractmethod
    def load(self, df): ...

    @abstractmethod
    def run(self): ...
