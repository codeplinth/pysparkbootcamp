from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .appName("test")
        .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print(spark.version)

spark.stop()