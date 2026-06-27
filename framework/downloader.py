from pathlib import Path

import requests

from framework.logger import LoggerManager


class Downloader:

    def __init__(self):

        self.logger = LoggerManager().get_logger()

    def download(self, dataset):

        destination = Path(dataset.destination)

        destination.mkdir(parents=True, exist_ok=True)

        output = destination / dataset.file_name

        self.logger.info(f"Baixando {dataset.name}")

        response = requests.get(dataset.url, timeout=60)

        response.raise_for_status()

        output.write_bytes(response.content)

        self.logger.success(f"Arquivo salvo em {output}")

        return output