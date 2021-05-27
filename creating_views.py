from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=(
    SparkSession
    .builder
    .appName("creating_views")
    .getOrCreate()
)

path="data/departuredelays.csv"
schema="date STRING,delay INT,distance INT,origin STRING,destination STRING"

flights_df=(
    spark.read.format("csv")
    .option("header","True")
    .schema(schema)
    .load(path)
)

flights_df.createOrReplaceTempView("us_flights_delay_view")
print(spark.catalog.listTables())

#SQL
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_sfo_global_temp_view AS 
SELECT date,delay,distance,origin,destination 
FROM us_flights_delay_view WHERE origin='SFO'""")

spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_jfk_temp_view AS 
SELECT date,delay,distance,origin,destination 
FROM us_flights_delay_view WHERE origin='JFK'""")

spark.sql("SELECT * FROM global_temp.us_origin_airport_sfo_global_temp_view").show(10)
spark.sql("SELECT * FROM us_origin_airport_jfk_temp_view").show(10)

#DataFrame
df_sfo=(
    flights_df.where(col("origin")=="SFO")
)

df_sfo.createOrReplaceTempView("us_origin_airport_sfo_view")
spark.sql("SELECT * FROM us_origin_airport_sfo_view").show(10)

spark.catalog.dropGlobalTempView("us_origin_airport_sfo_global_temp_view")
spark.catalog.dropTempView("us_origin_airport_jfk_view")
spark.catalog.dropTempView("us_origin_airport_sfo_view")

spark.stop()