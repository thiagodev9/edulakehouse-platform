from framework.config import ConfigManager

config = ConfigManager()

dataset = config.get_dataset("enem")

print(dataset)