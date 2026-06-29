"""
Downloads the IBGE municipios dataset to data/landing/ibge/municipios.json.

Used in CI before the Bronze pipeline and locally for first-time setup:
    python scripts/download/download_dataset.py
"""

import json
import sys
from pathlib import Path

import requests

IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
LANDING_PATH = Path("data/landing/ibge/municipios.json")


def download(url: str = IBGE_URL, dest: Path = LANDING_PATH) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    data = response.json()
    dest.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Downloaded {len(data)} municipalities → {dest}")
    return dest


if __name__ == "__main__":
    try:
        download()
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)
