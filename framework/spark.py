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

        self.spark = (
            SparkSession.builder
            .appName("EduLakehouse")
            .master("local[*]")
            .getOrCreate()
        )

    def get_session(self):

        return self.spark