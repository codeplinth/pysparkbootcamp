from pyspark.sql import SparkSession

spark=(
    SparkSession
    .builder
    .appName("creating_managed_tables")
    .getOrCreate()
)

print(spark.catalog.listDatabases())
print(spark.catalog.listTables())

spark.stop()