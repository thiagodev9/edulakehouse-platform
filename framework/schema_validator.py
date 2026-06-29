from framework.logger import LoggerManager


class SchemaValidator:

    @staticmethod
    def validate(df, expected_columns):

        logger = LoggerManager().get_logger()

        current_columns = set(df.columns)
        expected_columns = set(expected_columns)

        missing_columns = expected_columns - current_columns
        new_columns = current_columns - expected_columns

        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            logger.error(f"Schema incompatível — colunas ausentes: {missing}")
            raise ValueError("Pipeline interrompida. Schema incompatível.")

        if new_columns:
            added = ", ".join(sorted(new_columns))
            logger.warning(f"Novas colunas detectadas (mudança aditiva aceita): {added}")
        else:
            logger.info("Schema validado com sucesso.")
