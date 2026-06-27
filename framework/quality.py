from pyspark.sql.functions import col


class DataQuality:

    @staticmethod
    def generate_report(df, primary_key):

        print()
        print("=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)

        total_records = df.count()

        duplicate_count = (
            df.groupBy(primary_key)
            .count()
            .filter("count > 1")
            .count()
        )

        null_columns = []

        invalid_rows = df.filter(col(primary_key).isNull())

        for column in df.columns:

            nulls = df.filter(
                col(column).isNull()
            ).count()

            if nulls > 0:

                null_columns.append((column, nulls))

                invalid_rows = invalid_rows.union(
                    df.filter(col(column).isNull())
                )

        invalid_rows = invalid_rows.dropDuplicates()

        invalid_count = invalid_rows.count()

        valid_records = total_records - invalid_count

        quality = round(
            (valid_records / total_records) * 100,
            2
        )

        status = "OK"

        if duplicate_count > 0 or invalid_count > 0:

            status = "WARNING"

        print(f"Total registros............... {total_records}")
        print(f"Registros válidos............ {valid_records}")
        print(f"Registros com NULL........... {invalid_count}")
        print(f"Duplicados................... {duplicate_count}")
        print(f"Taxa de qualidade............ {quality}%")
        print(f"Status....................... {status}")

        if invalid_count > 0:

            print()
            print("=" * 60)
            print("DETALHES")
            print("=" * 60)

            rows = invalid_rows.collect()

            for row in rows:

                print()

                print(
                    f"{row.municipio_nome} ({row.municipio_id})"
                )

                print()

                print("Campos nulos:")

                for column, _ in null_columns:

                    if row[column] is None:

                        print(f" - {column}")

                print("-" * 40)

        return {

            "records": total_records,

            "valid_records": valid_records,

            "null_records": invalid_count,

            "duplicates": duplicate_count,

            "quality": quality,

            "status": status

        }