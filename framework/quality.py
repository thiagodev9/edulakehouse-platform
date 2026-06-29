from pyspark.sql.functions import col, count, when

from framework.logger import LoggerManager


class DataQuality:

    @staticmethod
    def generate_report(df, primary_key):

        logger = LoggerManager().get_logger()

        total_records = df.count()

        duplicate_count = (
            df.groupBy(primary_key)
            .count()
            .filter("count > 1")
            .count()
        )

        null_result = (
            df.select([
                count(when(col(c).isNull(), c)).alias(c)
                for c in df.columns
            ])
            .collect()[0]
        )

        null_columns = [
            (column, null_result[column])
            for column in df.columns
            if null_result[column] > 0
        ]

        invalid_rows = df.filter(col(primary_key).isNull())
        for column, _ in null_columns:
            invalid_rows = invalid_rows.union(df.filter(col(column).isNull()))
        invalid_rows = invalid_rows.dropDuplicates()
        invalid_count = invalid_rows.count()

        valid_records = total_records - invalid_count
        quality = (
            round((valid_records / total_records) * 100, 2)
            if total_records > 0 else 0.0
        )

        if duplicate_count == 0 and invalid_count == 0:
            status = "SUCCESS"
        elif quality >= 99:
            status = "WARNING"
        elif quality >= 95:
            status = "ERROR"
        else:
            status = "CRITICAL"

        if invalid_count > 0:
            rows = invalid_rows.collect()
            for row in rows:
                fields = ", ".join(
                    col_name for col_name, _ in null_columns
                    if row[col_name] is None
                )
                logger.warning(
                    f"Registro inválido | id={row[primary_key]}"
                    + (f" | campos nulos: {fields}" if fields else "")
                )
        else:
            logger.success("Nenhum problema de qualidade encontrado.")

        logger.info(
            f"Quality | total={total_records}"
            f" | válidos={valid_records}"
            f" | inválidos={invalid_count}"
            f" | duplicados={duplicate_count}"
            f" | qualidade={quality}%"
            f" | status={status}"
        )

        return {
            "records": total_records,
            "valid_records": valid_records,
            "null_records": invalid_count,
            "duplicates": duplicate_count,
            "quality": quality,
            "status": status,
            "columns_with_null": len(null_columns),
        }

    @staticmethod
    def save(report, output_dir="logs/quality"):
        import json
        from pathlib import Path
        from datetime import datetime

        logger = LoggerManager().get_logger()

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        filename = f"quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = path / filename

        keys_to_save = [
            "records", "valid_records", "duplicates",
            "null_records", "quality", "status",
        ]
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                {k: report[k] for k in keys_to_save if k in report},
                f, indent=4, ensure_ascii=False,
            )

        logger.info(f"Quality salvo em: {file_path.as_posix()}")
        return file_path.as_posix()
