import json
from datetime import datetime
from pathlib import Path

from framework.config import PIPELINE_VERSION
from framework.logger import LoggerManager


class AuditManager:

    @staticmethod
    def generate(pipeline_name, status, records, quality,
                 duration, version=PIPELINE_VERSION):

        return {
            "pipeline": pipeline_name,
            "status": status,
            "records": records,
            "quality": quality,
            "execution_time": round(duration, 2),
            "version": version,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def log(audit):

        logger = LoggerManager().get_logger()

        logger.info(
            "\n"
            + "=" * 60 + "\n"
            + "PIPELINE AUDIT\n"
            + "=" * 60 + "\n"
            + f"Pipeline.............. {audit['pipeline']}\n"
            + f"Status................ {audit['status']}\n"
            + f"Registros............. {audit['records']}\n"
            + f"Qualidade............. {audit['quality']}%\n"
            + f"Duração............... {audit['execution_time']} s\n"
            + f"Versão................ {audit['version']}\n"
            + f"Executado em.......... {audit['timestamp']}"
        )

    @staticmethod
    def save(audit, output_dir="logs/audit"):

        logger = LoggerManager().get_logger()

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        try:
            ts_dt = datetime.strptime(audit["timestamp"], "%Y-%m-%d %H:%M:%S")
            ts_str = ts_dt.strftime("%Y%m%d_%H%M%S")
        except Exception:
            ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = path / f"audit_{ts_str}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump({
                "pipeline": audit["pipeline"],
                "status": audit["status"],
                "records": audit["records"],
                "quality": audit["quality"],
                "duration": audit["execution_time"],
            }, f, indent=4, ensure_ascii=False)

        logger.info(f"Audit salvo em: {file_path.as_posix()}")
        return file_path.as_posix()
