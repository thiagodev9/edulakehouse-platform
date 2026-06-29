import json
import time
import uuid
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from framework.audit import AuditManager
from framework.base_pipeline import BasePipeline
from framework.config import (
    PIPELINE_VERSION,
    SAVE_AUDIT,
    SAVE_QUALITY,
    SAVE_METRICS
)
from framework.delta_utils import DeltaUtils
from framework.logger import LoggerManager
from framework.monitoring import PipelineMonitor


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

        logger = LoggerManager().get_logger()

        logger.info(
            "\n"
            + "=" * 60 + "\n"
            + "DATA QUALITY REPORT - GOLD\n"
            + "=" * 60 + "\n"
            + f"Total UFs................. {report['total_ufs']}\n"
            + f"UFs duplicadas............ {report['duplicate_ufs']}\n"
            + f"UFs nulas/vazias.......... {report['null_ufs']}\n"
            + f"Total negativo............ {report['negative_totals']}\n"
            + f"Total igual zero.......... {report['zero_totals']}\n"
            + f"Taxa de qualidade......... {report['quality']}%\n"
            + f"Status.................... {report['status']}"
        )

    @staticmethod
    def save(report, output_dir="logs/quality"):

        logger = LoggerManager().get_logger()

        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        filename = f"quality_gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = path / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)

        logger.info(f"Quality Gold salvo em: {file_path.as_posix()}")

        return file_path.as_posix()


###############################################################
# PIPELINE
###############################################################

class IBGEGoldPipeline(BasePipeline):

    def __init__(self):

        super().__init__()
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
    # SUMMARY
    ###########################################################

    def _log_summary(self, *, run_id, timings, record_count, quality_report,
                     written_files, audit_path, metrics_path, quality_path):

        t = timings
        self.logger.info(
            "\n"
            + "=" * 60 + "\n"
            + "PIPELINE SUMMARY\n"
            + "=" * 60 + "\n"
            + f"Extract............ {t['extract']:.2f}s\n"
            + f"Transform.......... {t['transform']:.2f}s\n"
            + f"Quality............ {t['quality']:.2f}s\n"
            + f"Load............... {t['load']:.2f}s\n"
            + f"Audit.............. {t['audit']:.2f}s\n"
            + "-" * 60 + "\n"
            + f"TOTAL.............. {t['pipeline']:.2f}s\n"
            + f"REGISTROS.......... {record_count}\n"
            + f"QUALIDADE.......... {quality_report['quality']}%\n"
            + f"STATUS............. {quality_report['status']}\n"
            + f"ARQUIVOS ESCRITOS.. {written_files}\n"
            + f"RUN ID............. {run_id}\n"
            + "\n"
            + "=" * 60 + "\n"
            + "OUTPUTS\n"
            + "=" * 60 + "\n"
            + f"Gold............... {self.output_path}\n"
            + (f"Audit.............. {audit_path}\n" if audit_path else "")
            + (f"Metrics............ {metrics_path}\n" if metrics_path else "")
            + (f"Quality............ {quality_path}\n" if quality_path else "")
            + f"Run ID............. {run_id}"
        )

    ###########################################################
    # RUN
    ###########################################################

    def run(self):

        run_id = uuid.uuid4().hex[:8]
        self.logger.info(f"Iniciando pipeline IBGE Gold | RUN ID: {run_id}")
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

            self.logger.info(f"Extract: {extract_time:.2f}s")

            ###################################################
            # TRANSFORM
            ###################################################

            transform_start = time.perf_counter()
            df_gold = self.transform(df_silver, run_id)
            df_gold.persist(StorageLevel.MEMORY_AND_DISK)
            transform_time = time.perf_counter() - transform_start

            self.logger.info(f"Transform: {transform_time:.2f}s")

            ###################################################
            # VIEWS ANALÍTICAS
            ###################################################

            self.logger.info("Municípios por Estado:")
            df_gold.select("uf_sigla", "uf_nome", "total_municipios").show(
                30, truncate=False
            )

            self.logger.info("Municípios por Região:")
            (
                df_gold
                .groupBy("regiao_nome")
                .agg(F.sum("total_municipios").alias("total_municipios"))
                .orderBy("regiao_nome")
                .show(truncate=False)
            )

            self.logger.info("Estados por Região:")
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
            quality_path = GoldDataQuality.save(quality_report) if SAVE_QUALITY else None
            quality_time = time.perf_counter() - quality_start

            # record_count vem do quality_report — sem Spark action extra
            record_count = quality_report["total_ufs"]

            self.logger.info(
                f"Quality: {quality_time:.2f}s"
                f" | {record_count} UFs"
                f" | {quality_report['quality']}%"
                f" | {quality_report['status']}"
            )

            ###################################################
            # LOAD
            ###################################################

            load_start = time.perf_counter()
            self.load(df_gold)
            load_time = time.perf_counter() - load_start

            written_files = len([
                f for f in Path(self.output_path).rglob("*.parquet")
                if "_delta_log" not in f.parts
            ])

            self.logger.info(
                f"Load: {load_time:.2f}s | {written_files} arquivos Parquet"
            )

            df_silver.unpersist()
            df_gold.unpersist()

            pipeline_time = time.perf_counter() - pipeline_start

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
                version=PIPELINE_VERSION,
            )

            AuditManager.log(audit)
            audit_path = AuditManager.save(audit) if SAVE_AUDIT else None
            audit_time = time.perf_counter() - audit_start

            ###################################################
            # METRICS
            ###################################################

            metrics_path = PipelineMonitor.save_metrics({
                "extract": extract_time,
                "transform": transform_time,
                "quality": quality_time,
                "load": load_time,
                "total": pipeline_time,
            }) if SAVE_METRICS else None

            ###################################################
            # SUMMARY
            ###################################################

            self._log_summary(
                run_id=run_id,
                timings={
                    "extract": extract_time,
                    "transform": transform_time,
                    "quality": quality_time,
                    "load": load_time,
                    "audit": audit_time,
                    "pipeline": pipeline_time,
                },
                record_count=record_count,
                quality_report=quality_report,
                written_files=written_files,
                audit_path=audit_path,
                metrics_path=metrics_path,
                quality_path=quality_path,
            )

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

            self.logger.exception(f"Erro durante execução da pipeline Gold: {e}")
            raise


if __name__ == "__main__":

    pipeline = IBGEGoldPipeline()

    pipeline.run()
