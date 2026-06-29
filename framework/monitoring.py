import json

from pathlib import Path

from datetime import datetime


class PipelineMonitor:

    @staticmethod
    def save_execution(
        pipeline_name,
        layer,
        status,
        records,
        quality,
        duplicates,
        execution_time
    ):

        Path(
            f"monitoring/{layer}"
        ).mkdir(
            parents=True,
            exist_ok=True
        )

        report = {

            "pipeline": pipeline_name,

            "layer": layer,

            "status": status,

            "records": records,

            "quality_percent": quality,

            "duplicates": duplicates,

            "execution_time_seconds": execution_time,

            "execution_date":

            datetime.now().strftime(

                "%Y-%m-%d %H:%M:%S"

            )

        }

        filename = (

            f"monitoring/{layer}/"

            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        )

        with open(

            filename,

            "w",

            encoding="utf-8"

        ) as file:

            json.dump(

                report,

                file,

                indent=4,

                ensure_ascii=False

            )

        print()

        print("=" * 60)

        print("MONITORAMENTO")

        print("=" * 60)

        print(f"Relatório salvo em:")

        print(filename)

        print()

    @staticmethod
    def save_metrics(metrics, output_dir="logs/metrics"):
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        filename = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = path / filename

        formatted_metrics = {k: round(v, 2) for k, v in metrics.items()}

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(formatted_metrics, file, indent=4, ensure_ascii=False)

        print()
        print("Log de métricas salvo em:")
        print(file_path.as_posix())

        return file_path.as_posix()