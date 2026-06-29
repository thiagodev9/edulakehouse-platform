import json
from datetime import datetime
from pathlib import Path

from framework.logger import LoggerManager


class PipelineMonitor:

    @staticmethod
    def save_execution(pipeline_name, layer, status, records,
                       quality, duplicates, execution_time):

        logger = LoggerManager().get_logger()

        Path(f"monitoring/{layer}").mkdir(parents=True, exist_ok=True)

        report = {
            "pipeline": pipeline_name,
            "layer": layer,
            "status": status,
            "records": records,
            "quality_percent": quality,
            "duplicates": duplicates,
            "execution_time_seconds": execution_time,
            "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        filename = f"monitoring/{layer}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(report, file, indent=4, ensure_ascii=False)

        logger.info(f"Monitoramento salvo em: {filename}")

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
