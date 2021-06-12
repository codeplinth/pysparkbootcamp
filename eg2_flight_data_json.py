from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,lit

spark=(
    SparkSession
    .builder
    .master("local[*]")
    .appName("eg_flight_data_json")
    .getOrCreate()
)

df=(
    spark.read.format("json")
    .load("data/flight_data/json/2015-summary.json")
)

df.printSchema()
df.createOrReplaceTempView("dfTable")

df.select("DEST_COUNTRY_NAME").show(2)

spark.sql("""
SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2
""").show()

df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

df.select(expr("DEST_COUNTRY_NAME as destination")).show(2)

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST")).show(2)

df.selectExpr("DEST_COUNTRY_NAME as newColumn","DEST_COUNTRY_NAME").show(4)

df.selectExpr(
    "*",
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry"
).show(5,truncate=False)

df.selectExpr(
    "AVG(count)",
    "COUNT(DISTINCT(DEST_COUNTRY_NAME))"
).show()

df.selectExpr(
    "*",
    "1 AS One"
).show(5)

df.withColumn("columnOne",lit(1)).show()

df.withColumn("withInCountry",expr("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME")).show(5)

df.withColumn("dest",expr("DEST_COUNTRY_NAME")).show(5)

dfwithLongColumn=df.withColumn("This is long column-name",expr("DEST_COUNTRY_NAME"))

dfwithLongColumn.selectExpr("`This is long column-name`","`This is long column-name` AS `Long Col`").show()

print(df.rdd.getNumPartitions())

spark.stop()