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
                    hadoop_bin = str(local_hadoop / "bin")
                    path_env = os.environ.get("PATH", "")
                    if hadoop_bin not in path_env.split(os.path.pathsep):
                        os.environ["PATH"] = f"{hadoop_bin}{os.path.pathsep}{path_env}"

        builder = (
            SparkSession.builder
            .appName("EduLakehouse")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "32")
        )

        # PySpark 4.0 + delta-spark 4.0 requires both the JAR on the classpath
        # (via configure_spark_with_delta_pip) AND explicit extension config.
        # configure_spark_with_delta_pip resolves the JAR via Ivy but does NOT
        # set the extensions in PySpark 4.0 — hence both blocks are needed.
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(
                builder
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
        except ImportError:
            pass  # delta-spark not installed; pipelines will use Parquet

        self.spark = builder.getOrCreate()

    def get_session(self):
        return self.spark
