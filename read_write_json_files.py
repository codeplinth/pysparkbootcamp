from pyspark.sql import SparkSession

spark=(
    SparkSession
    .builder
    .appName("read_write_json_files")
    .getOrCreate()
)

df=(
    spark.read.format("json")
    .option("mode","FAILFAST")
    .option("inferSchema","true")
    .load("data/flight_data/json/2015-summary.json")
)

df.printSchema()
df.show()

df.write.format("json").mode("overwrite").save("tmp/my_json")

spark.stop()