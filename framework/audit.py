from datetime import datetime
from framework.config import PIPELINE_VERSION


class AuditManager:

    @staticmethod
    def generate(
        pipeline_name,
        status,
        records,
        quality,
        duration,
        version=PIPELINE_VERSION
    ):

        start = datetime.now()

        audit = {

            "pipeline": pipeline_name,

            "status": status,

            "records": records,

            "quality": quality,

            "execution_time": round(duration, 2),

            "version": version,

            "timestamp": start.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        }

        return audit

    @staticmethod
    def print(audit):

        print()

        print("=" * 60)

        print("PIPELINE AUDIT")

        print("=" * 60)

        print(f"Pipeline.............. {audit['pipeline']}")

        print(f"Status................ {audit['status']}")

        print(f"Registros............. {audit['records']}")

        print(f"Qualidade............. {audit['quality']}%")

        print(f"Duração............... {audit['execution_time']} s")

        print(f"Versão................ {audit['version']}")

        print(f"Executado em.......... {audit['timestamp']}")

    @staticmethod
    def save(audit, output_dir="logs/audit"):
        import json
        from pathlib import Path

        # Create output directory if it doesn't exist
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        # Parse timestamp to match YYYYMMDD_HHMMSS format for filename
        try:
            ts_dt = datetime.strptime(audit["timestamp"], "%Y-%m-%d %H:%M:%S")
            ts_str = ts_dt.strftime("%Y%m%d_%H%M%S")
        except Exception:
            ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = path / f"audit_{ts_str}.json"

        # Prepare details matching the user's expected output schema
        report = {
            "pipeline": audit["pipeline"],
            "status": audit["status"],
            "records": audit["records"],
            "quality": audit["quality"],
            "duration": audit["execution_time"]
        }

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)

        print()
        print(f"Log de auditoria salvo em:")
        print(file_path.as_posix())

        return file_path.as_posix()