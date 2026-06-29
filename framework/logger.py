from loguru import logger
import sys
from pathlib import Path

_initialized = False


class LoggerManager:

    def __init__(self):
        global _initialized
        if not _initialized:
            Path("logs").mkdir(exist_ok=True)
            logger.remove()
            logger.add(
                sys.stdout,
                format=(
                    "{time:YYYY-MM-DD HH:mm:ss} | "
                    "{level:<8} | "
                    "{message}"
                ),
                level="INFO",
            )
            logger.add(
                "logs/pipeline.log",
                rotation="10 MB",
                retention="30 days",
                compression="zip",
                level="INFO",
            )
            _initialized = True
        self.logger = logger

    def get_logger(self):
        return self.logger
