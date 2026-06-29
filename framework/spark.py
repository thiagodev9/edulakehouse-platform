import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


class SparkManager:

    def __init__(self):

        if sys.platform == "win32" or os.name == "nt":

            if not os.environ.get("HADOOP_HOME"):

                project_root = Path(__file__).resolve().parents[1]

                local_hadoop = project_root / "hadoop"

                if (local_hadoop / "bin" / "winutils.exe").exists():

                    os.environ["HADOOP_HOME"] = str(local_hadoop)

                    path_env = os.environ.get("PATH", "")

                    hadoop_bin = str(local_hadoop / "bin")

                    if hadoop_bin not in path_env.split(os.path.pathsep):

                        os.environ["PATH"] = (
                            f"{hadoop_bin}{os.path.pathsep}{path_env}"
                        )

        builder = (
            SparkSession.builder
            .appName("EduLakehouse")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "32")
        )

        # Sprint 23 — Delta Lake
        # PySpark 4.0 + delta-spark 4.0 exige tanto o JAR no classpath (via
        # configure_spark_with_delta_pip) quanto as extensões configuradas
        # explicitamente. configure_spark_with_delta_pip resolve o JAR via Ivy,
        # mas não seta as extensões no PySpark 4.0 — por isso os dois blocos.
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(
                builder
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
        except ImportError:
            pass  # delta-spark não instalado; pipelines usarão Parquet

        self.spark = builder.getOrCreate()

    def get_session(self):

        return self.spark