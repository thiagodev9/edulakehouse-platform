from pathlib import Path
from loguru import logger

from framework.config import ConfigManager


class LoggerManager:
    """
    Gerencia os logs do projeto.
    """

    def __init__(self):

        config = ConfigManager()

        log_path = Path(config.get("paths.logs"))

        log_path.mkdir(parents=True, exist_ok=True)

        logger.remove()

        logger.add(
            log_path / "pipeline.log",
            level=config.get("logging.level"),
            rotation="10 MB",
            retention="30 days",
            compression="zip",
            enqueue=True,
        )

        logger.add(
            sink=lambda msg: print(msg, end=""),
            level=config.get("logging.level"),
        )

        self.logger = logger

    def get_logger(self):

        return self.logger