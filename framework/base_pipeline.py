import time
from abc import ABC, abstractmethod

from framework.config import ConfigManager
from framework.logger import LoggerManager


class BasePipeline(ABC):

    def __init__(self):

        self.config = ConfigManager()

        self.logger = LoggerManager().get_logger()

    @abstractmethod
    def run(self):
        """
        Método obrigatório.
        """
        pass

    def execute(self):

        start = time.time()

        self.logger.info("Pipeline iniciado")

        try:

            self.run()

            elapsed = round(time.time() - start, 2)

            self.logger.success(
                f"Pipeline finalizado em {elapsed} segundos."
            )

        except Exception as e:

            self.logger.exception(e)

            raise