from pathlib import Path
import yaml

from framework.dataset import Dataset

PIPELINE_VERSION = "1.0.0"
REPARTITIONS = 4
DEBUG = True
SHOW_SAMPLE = True
SHOW_SCHEMA = True
SAVE_AUDIT = True
SAVE_QUALITY = True
SAVE_METRICS = True


class ConfigManager:
    """
    Responsável por carregar as configurações do projeto.
    """

    def __init__(self, config_path="config/config.yaml"):

        self.config_path = Path(config_path)

        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {self.config_path}"
            )

        with open(self.config_path, "r", encoding="utf-8") as file:
            self.config = yaml.safe_load(file)

    def get(self, key):

        keys = key.split(".")

        value = self.config

        for item in keys:
            value = value[item]

        return value

    def get_dataset(self, dataset_name) -> Dataset:

        with open(
            "config/datasets.yaml",
            "r",
            encoding="utf-8"
        ) as file:

            datasets = yaml.safe_load(file)

        data = datasets["datasets"][dataset_name]

        return Dataset(**data)