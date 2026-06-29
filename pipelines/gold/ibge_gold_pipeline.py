import json
import time
import uuid
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from framework.audit import AuditManager
from framework.config import (
    PIPELINE_VERSION,
    SAVE_AUDIT,
    SAVE_QUALITY,
    SAVE_METRICS
)
from framework.delta_utils import DeltaUtils
from framework.logger import LoggerManager
from framework.monitoring import PipelineMonitor
from framework.spark import SparkManager


###############################################################
# GOLD DATA QUALITY
###############################################################

class GoldDataQuality:

    @staticmethod
    def check(df):

        total_ufs = df.count()

        duplicate_ufs = (
            df.groupBy("uf_sigla")
            .count()
            .filter("count > 1")
            .count()
        )

        null_ufs = df.filter(
            F.col("uf_sigla").isNull()
            | (F.trim(F.col("uf_sigla")) == "")
        ).count()

        negative_totals = df.filter(
            F.col("total_municipios") < 0
        ).count()

        zero_totals = df.filter(
            F.col("total_municipios") == 0
        ).count()

        invalid_records = df.filter(
            F.col("uf_sigla").isNull()
            | (F.trim(F.col("uf_sigla")) == "")
            | (F.col("total_municipios") <= 0)
        ).count()

        quality = (
            round(((total_ufs - invalid_records) / total_ufs) * 100, 2)
            if total_ufs > 0 else 0.0
        )

        if duplicate_ufs == 0 and invalid_records == 0:
            status = "SUCCESS"
        elif quality >= 99:
            status = "WARNING"
        elif quality >= 95:
            status = "ERROR"
        else:
            status = "CRITICAL"

        return {
            "total_ufs": total_ufs,
            "duplicate_ufs": duplicate_ufs,
            "null_ufs": null_ufs,
            "negative_totals": negative_totals,
            "zero_totals": zero_totals,
            "invalid_records": invalid_records,
            "quality": quality,
            "status": status
        }

    @staticmethod
    def print_report(report):

        print()
        print("=" * 60)
        print("DATA QUALITY REPORT - GOLD")
        print("=" * 60)
        print(f"Total UFs................. {report['total_ufs']}")
        print(f"UFs duplicadas............ {report['duplicate_ufs']}")
        print(f"UFs nulas/vazias.......... {report['null_ufs']}")
        print(f"Total negativo............ {report['negative_totals']}")
        print(f"Total igual zero.......... {report['zero_totals']}")
        print(f"Taxa de qualidade......... {report['quality']}%")
        print(f"Status.................... {report['status']}")

    @staticmethod
    def save(report, output_dir="logs/quality"):

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        filename = f"quality_gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = path / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)

        print()
        print("Relatório de qualidade Gold salvo em:")
        print(file_path.as_posix())

        return file_path.as_posix()


###############################################################
# PIPELINE
###############################################################

class IBGEGoldPipeline:

    def __init__(self):

        self.spark = SparkManager().get_session()

        self.logger = LoggerManager().get_logger()

        self.input_path = "data/silver/ibge"

        self.output_path = "data/gold/ibge_dashboard"

    ###########################################################
    # EXTRACT
    ###########################################################

    def extract(self):

        self.logger.info("Lendo Silver...")

        if DeltaUtils.is_delta_table(self.spark, self.input_path):
            self.logger.info("Formato detectado: Delta Lake")
            return DeltaUtils.read(self.spark, self.input_path)

        self.logger.info("Formato detectado: Parquet (execute Silver para migrar para Delta)")
        return self.spark.read.parquet(self.input_path)

    ###########################################################
    # TRANSFORM
    ###########################################################

    def transform(self, df, run_id):

        self.logger.info("Agregando municípios por UF...")

        data_processamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return (
            df.groupBy("uf_sigla", "uf_nome", "regiao_nome")
            .agg(F.count("municipio_id").alias("total_municipios"))
            .withColumn("data_processamento", F.lit(data_processamento))
            .withColumn("run_id", F.lit(run_id))
            .orderBy("uf_sigla")
        )

    ###########################################################
    # LOAD
    ###########################################################

    def load(self, df):

        self.logger.info("Gravando Gold — Delta Lake (particionado por regiao_nome)...")

        DeltaUtils.write_or_merge(
            self.spark,
            df.repartition("regiao_nome"),
            self.output_path,
            merge_condition="target.uf_sigla = source.uf_sigla",
            partition_by=["regiao_nome"],
        )

        self.logger.info("Executando OPTIMIZE...")
        DeltaUtils.optimize(self.spark, self.output_path)

        self.logger.info("Executando VACUUM (retenção 7 dias)...")
        DeltaUtils.vacuum(self.spark, self.output_path, retention_hours=168)

    ###########################################################
    # RUN
    ###########################################################

    def run(self):

        run_id = uuid.uuid4().hex[:8]

        self.logger.info(
            f"Iniciando pipeline IBGE Gold | RUN ID: {run_id}"
        )

        pipeline_start = time.perf_counter()

        df_silver = None
        df_gold = None

        try:

            ###################################################
            # EXTRACT
            ###################################################

            extract_start = time.perf_counter()

            df_silver = self.extract()

            df_silver.persist(StorageLevel.MEMORY_AND_DISK)

            extract_time = time.perf_counter() - extract_start

            self.logger.info(f"Extract finalizado em {extract_time:.2f}s")

            ###################################################
            # TRANSFORM
            ###################################################

            transform_start = time.perf_counter()

            df_gold = self.transform(df_silver, run_id)

            df_gold.persist(StorageLevel.MEMORY_AND_DISK)

            record_count = df_gold.count()

            transform_time = time.perf_counter() - transform_start

            self.logger.info(
                f"Transform finalizado em {transform_time:.2f}s | UFs: {record_count}"
            )

            ###################################################
            # VIEWS ANALÍTICAS
            ###################################################

            print()
            print("=" * 60)
            print("MUNICÍPIOS POR ESTADO")
            print("=" * 60)

            df_gold.select(
                "uf_sigla", "uf_nome", "total_municipios"
            ).show(record_count, truncate=False)

            print()
            print("=" * 60)
            print("MUNICÍPIOS POR REGIÃO")
            print("=" * 60)

            (
                df_gold
                .groupBy("regiao_nome")
                .agg(F.sum("total_municipios").alias("total_municipios"))
                .orderBy("regiao_nome")
                .show(truncate=False)
            )

            print()
            print("=" * 60)
            print("ESTADOS POR REGIÃO")
            print("=" * 60)

            (
                df_gold
                .groupBy("regiao_nome")
                .agg(F.count("uf_sigla").alias("total_estados"))
                .orderBy("regiao_nome")
                .show(truncate=False)
            )

            ###################################################
            # DATA QUALITY
            ###################################################

            quality_start = time.perf_counter()

            quality_report = GoldDataQuality.check(df_gold)

            GoldDataQuality.print_report(quality_report)

            quality_path = (
                GoldDataQuality.save(quality_report)
                if SAVE_QUALITY else None
            )

            quality_time = time.perf_counter() - quality_start

            self.logger.info(
                f"Data Quality executada em {quality_time:.2f}s"
            )

            ###################################################
            # LOAD
            ###################################################

            load_start = time.perf_counter()

            self.load(df_gold)

            load_time = time.perf_counter() - load_start

            # Conta apenas arquivos de dados, excluindo _delta_log (checkpoints)
            written_files = len([
                f for f in Path(self.output_path).rglob("*.parquet")
                if "_delta_log" not in f.parts
            ])

            self.logger.info(
                f"Load finalizado em {load_time:.2f}s"
                f" | Arquivos Parquet: {written_files}"
            )

            df_silver.unpersist()
            df_gold.unpersist()

            ###################################################
            # PIPELINE TOTAL
            ###################################################

            pipeline_time = time.perf_counter() - pipeline_start

            print()
            print(f"Dados gravados em: {self.output_path}")
            print()

            self.logger.success("Pipeline Gold finalizada com sucesso.")

            ###################################################
            # AUDIT
            ###################################################

            audit_start = time.perf_counter()

            audit = AuditManager.generate(
                pipeline_name="IBGE Gold",
                status=quality_report["status"],
                records=record_count,
                quality=quality_report["quality"],
                duration=pipeline_time,
                version=PIPELINE_VERSION
            )

            AuditManager.print(audit)

            audit_path = (
                AuditManager.save(audit) if SAVE_AUDIT else None
            )

            audit_time = time.perf_counter() - audit_start

            ###################################################
            # METRICS
            ###################################################

            metrics = {
                "extract": extract_time,
                "transform": transform_time,
                "quality": quality_time,
                "load": load_time,
                "total": pipeline_time
            }

            metrics_path = (
                PipelineMonitor.save_metrics(metrics)
                if SAVE_METRICS else None
            )

            ###################################################
            # PIPELINE SUMMARY
            ###################################################

            print()
            print("=" * 60)
            print("PIPELINE SUMMARY")
            print("=" * 60)

            print(f"Extract............ {extract_time:.2f}s")
            print(f"Transform.......... {transform_time:.2f}s")
            print(f"Quality............ {quality_time:.2f}s")
            print(f"Load............... {load_time:.2f}s")
            print(f"Audit.............. {audit_time:.2f}s")

            print("-" * 60)

            print(f"TOTAL.............. {pipeline_time:.2f}s")
            print(f"REGISTROS.......... {record_count}")
            print(f"QUALIDADE.......... {quality_report['quality']}%")
            print(f"STATUS............. {quality_report['status']}")
            print(f"ARQUIVOS ESCRITOS.. {written_files}")
            print(f"RUN ID............. {run_id}")

            ###################################################
            # OUTPUTS
            ###################################################

            print()
            print("=" * 60)
            print("OUTPUTS")
            print("=" * 60)

            print(f"Gold............... {self.output_path}")

            if audit_path:
                print(f"Audit.............. {audit_path}")

            if metrics_path:
                print(f"Metrics............ {metrics_path}")

            if quality_path:
                print(f"Quality............ {quality_path}")

            print(f"Run ID............. {run_id}")

            self.logger.info(
                f"Pipeline executada em {pipeline_time:.2f}s | RUN ID: {run_id}"
            )

        except Exception as e:

            if df_gold is not None:
                try:
                    df_gold.unpersist()
                except Exception:
                    pass

            if df_silver is not None:
                try:
                    df_silver.unpersist()
                except Exception:
                    pass

            self.logger.exception(
                f"Erro durante execução da pipeline Gold: {e}"
            )

            raise


if __name__ == "__main__":

    pipeline = IBGEGoldPipeline()

    pipeline.run()
