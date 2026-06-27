from dataclasses import dataclass


@dataclass
class Dataset:

    name: str

    url: str

    destination: str

    file_name: str