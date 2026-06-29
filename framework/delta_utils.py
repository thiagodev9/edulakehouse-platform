"""
Utilitários Delta Lake: MERGE, TIME TRAVEL, OPTIMIZE, VACUUM, HISTORY.

Requer delta-spark instalado e SparkSession configurado com:
  spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
  spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
"""

from pathlib import Path


class DeltaUtils:

    ###########################################################
    # VERIFICAÇÃO
    ###########################################################

    @staticmethod
    def is_delta_table(spark, path):
        """Retorna True se o path já contém uma Delta table."""
        from delta.tables import DeltaTable
        return Path(path).exists() and DeltaTable.isDeltaTable(spark, path)

    ###########################################################
    # WRITE / MERGE
    ###########################################################

    @staticmethod
    def write_overwrite(df, path, partition_by=None):
        """Primeira escrita: cria a Delta table do zero."""
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    @staticmethod
    def merge_into(spark, target_path, source_df, merge_condition):
        """
        Upsert incremental: atualiza linhas existentes e insere novas.

        Exemplo:
            DeltaUtils.merge_into(
                spark,
                "data/silver/ibge",
                df_new,
                "target.municipio_id = source.municipio_id"
            )
        """
        from delta.tables import DeltaTable

        target = DeltaTable.forPath(spark, target_path)

        (
            target.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

    @staticmethod
    def write_or_merge(spark, df, path, merge_condition, partition_by=None):
        """
        Abstração: primeira execução cria a tabela (overwrite),
        execuções seguintes fazem MERGE incremental.
        """
        if DeltaUtils.is_delta_table(spark, path):
            DeltaUtils.merge_into(spark, path, df, merge_condition)
        else:
            DeltaUtils.write_overwrite(df, path, partition_by)

    ###########################################################
    # TIME TRAVEL
    ###########################################################

    @staticmethod
    def read(spark, path, version=None, timestamp=None):
        """
        Leitura com time travel opcional.

        Exemplos:
            DeltaUtils.read(spark, path)                        # versão atual
            DeltaUtils.read(spark, path, version=0)             # versão inicial
            DeltaUtils.read(spark, path, timestamp="2025-01-01")  # por data
        """
        reader = spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", str(timestamp))

        return reader.load(path)

    ###########################################################
    # MANUTENÇÃO
    ###########################################################

    @staticmethod
    def optimize(spark, path):
        """
        Compacta small files em arquivos maiores.
        Equivalente ao OPTIMIZE do Databricks.
        """
        from delta.tables import DeltaTable
        DeltaTable.forPath(spark, path).optimize().executeCompaction()

    @staticmethod
    def vacuum(spark, path, retention_hours=168):
        """
        Remove arquivos antigos não referenciados.
        retention_hours=168 → mantém 7 dias de histórico.

        ATENÇÃO: reduzir abaixo de 168h remove a capacidade de time travel
        para versões antigas.
        """
        from delta.tables import DeltaTable

        # Desabilita a verificação de retenção mínima para ambientes locais
        spark.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled",
            "false"
        )

        DeltaTable.forPath(spark, path).vacuum(retention_hours)

    ###########################################################
    # HISTÓRICO
    ###########################################################

    @staticmethod
    def get_history(spark, path, limit=10):
        """
        Retorna DataFrame com o histórico de operações da Delta table.
        Inclui: version, timestamp, operation, operationParameters.
        """
        from delta.tables import DeltaTable
        return DeltaTable.forPath(spark, path).history(limit)

    @staticmethod
    def print_history(spark, path, limit=10):
        """Imprime o histórico de versões da Delta table."""
        print()
        print("=" * 60)
        print("DELTA TABLE HISTORY")
        print("=" * 60)
        DeltaUtils.get_history(spark, path, limit).select(
            "version", "timestamp", "operation"
        ).show(truncate=False)
