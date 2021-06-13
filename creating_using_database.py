from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("reading_managed_table")
    .getOrCreate()
)

print(spark.catalog.listDatabases())

spark.sql("create database if not exists learn_spark_db")

print(spark.catalog.listDatabases())

spark.sql("use learn_spark_db")

print(spark.catalog.listTables())

spark.stop()