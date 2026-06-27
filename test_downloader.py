from framework.config import ConfigManager
from framework.downloader import Downloader
from framework.dataset import Dataset

config = ConfigManager()

dataset = config.get_dataset("ibge")

downloader = Downloader()

file = downloader.download(dataset)

print(file)