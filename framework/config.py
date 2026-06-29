from pathlib import Path
import yaml

from framework.dataset import Dataset

def _load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _cfg():
    try:
        return _load_config()
    except FileNotFoundError:
        return {}


_c = _cfg()

PIPELINE_VERSION = _c.get("project", {}).get("version", "1.0.0")
REPARTITIONS = _c.get("pipeline", {}).get("repartitions", 4)
DEBUG = _c.get("debug", {}).get("show_nulls", True)
SHOW_SCHEMA = _c.get("debug", {}).get("show_schema", True)
SHOW_SAMPLE = _c.get("debug", {}).get("show_sample", True)
SAVE_AUDIT = _c.get("save", {}).get("audit", True)
SAVE_QUALITY = _c.get("save", {}).get("quality", True)
SAVE_METRICS = _c.get("save", {}).get("metrics", True)


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