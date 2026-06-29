import json
from datetime import datetime
from pathlib import Path

from framework.logger import LoggerManager


class PipelineMonitor:

    @staticmethod
    def save_metrics(metrics, output_dir="logs/metrics"):

        logger = LoggerManager().get_logger()

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        filename = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = path / filename

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(
                {k: round(v, 2) for k, v in metrics.items()},
                file, indent=4, ensure_ascii=False,
            )

        logger.info(f"Métricas salvas em: {file_path.as_posix()}")
        return file_path.as_posix()
