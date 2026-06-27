from framework.spark import SparkManager

spark = SparkManager().get_session()

print(spark.version)