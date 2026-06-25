from framework.config import ConfigManager


config = ConfigManager()

print(config.get("project.name"))

print(config.get("paths.bronze"))

print(config.get("logging.level"))