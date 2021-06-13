from pyspark.sql import SparkSession
from pyspark.sql.functions import instr

spark = (SparkSession
        .builder
        .appName("test")
        .getOrCreate())

#using delta
#spark = (SparkSession.builder.appName("MyApp") \
#    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
#    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(spark.sparkContext.uiWebUrl)
print(spark.version)

spark.stop()

