import yaml


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