from pyspark.sql.functions import (
    col,
    count,
    when
)
from loguru import logger


class DataQuality:

    @staticmethod
    def generate_report(df, primary_key):

        print()
        print("=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)

        ###########################################################
        # TOTAL DE REGISTROS
        ###########################################################

        total_records = df.count()

        ###########################################################
        # DUPLICADOS
        ###########################################################

        duplicate_count = (
            df.groupBy(primary_key)
            .count()
            .filter("count > 1")
            .count()
        )

        ###########################################################
        # CONTAGEM DE NULLS (1 ÚNICA LEITURA)
        ###########################################################

        null_result = (
            df.select([
                count(
                    when(col(c).isNull(), c)
                ).alias(c)
                for c in df.columns
            ])
            .collect()[0]
        )

        null_columns = []

        for column in df.columns:

            if null_result[column] > 0:

                null_columns.append(
                    (column, null_result[column])
                )

        ###########################################################
        # REGISTROS INVÁLIDOS
        ###########################################################

        invalid_rows = df.filter(
            col(primary_key).isNull()
        )

        for column, _ in null_columns:

            invalid_rows = invalid_rows.union(

                df.filter(
                    col(column).isNull()
                )

            )

        invalid_rows = invalid_rows.dropDuplicates()

        invalid_count = invalid_rows.count()

        ###########################################################
        # MÉTRICAS
        ###########################################################

        valid_records = total_records - invalid_count

        quality = round(

            (valid_records / total_records) * 100,

            2

        ) if total_records > 0 else 0

        if duplicate_count == 0 and invalid_count == 0:

            status = "OK"

        elif quality >= 99:

            status = "WARNING"

        elif quality >= 95:

            status = "ERROR"

        else:

            status = "CRITICAL"

        ###########################################################
        # RESUMO
        ###########################################################

        print(f"Total registros............... {total_records}")
        print(f"Registros válidos............ {valid_records}")
        print(f"Registros com NULL........... {invalid_count}")
        print(f"Duplicados................... {duplicate_count}")
        print(f"Taxa de qualidade............ {quality}%")
        print(f"Status....................... {status}")

        ###########################################################
        # DETALHES
        ###########################################################

        if invalid_count > 0:

            print()
            print("=" * 60)
            print("DETALHES")
            print("=" * 60)

            rows = invalid_rows.collect()

            for row in rows:

                logger.warning(

                    "Registro inválido | "

                    f"id={row.municipio_id} | "

                    f"municipio={row.municipio_nome}"

                )

                print()

                print(
                    f"{row.municipio_nome} "
                    f"({row.municipio_id})"
                )

                print()

                print("Campos nulos:")

                for column, _ in null_columns:

                    if row[column] is None:

                        print(f" - {column}")

                print("-" * 40)

        else:

            logger.success(
                "Nenhum problema de qualidade encontrado."
            )

            print()
            print("Nenhum problema encontrado.")

        ###########################################################
        # LOG FINAL
        ###########################################################

        logger.info(
            f"""
Resumo Data Quality
-------------------
Registros........... {total_records}
Válidos............. {valid_records}
Inválidos........... {invalid_count}
Duplicados.......... {duplicate_count}
Qualidade........... {quality}%
Status.............. {status}
"""
        )

        ###########################################################
        # RETORNO
        ###########################################################

        return {

            "records": total_records,

            "valid_records": valid_records,

            "null_records": invalid_count,

            "duplicates": duplicate_count,

            "quality": quality,

            "status": status,

            "columns_with_null": len(null_columns)

        }

    @staticmethod
    def save(report, output_dir="logs/quality"):
        import json
        from pathlib import Path
        from datetime import datetime

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        filename = f"quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = path / filename

        keys_to_save = ["records", "valid_records", "duplicates", "null_records", "quality", "status"]
        data_to_save = {k: report[k] for k in keys_to_save if k in report}

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)

        print()
        print(f"Relatório de qualidade salvo em:")
        print(file_path.as_posix())

        return file_path.as_posix()