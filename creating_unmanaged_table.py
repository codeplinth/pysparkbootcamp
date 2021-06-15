from pyspark.sql import SparkSession

spark=(
    SparkSession
    .builder
    .config("spark.sql.warehouse.dir","spark-warehouse")
    .appName("creating_unmanaged_table")
    .getOrCreate()
)

#create a unmanaged table using Spark SQL
spark.sql("""CREATE TABLE us_flight_delays_tbl(
                date STRING,
                delay INT,
                distance INT,
                origin STRING,
                destination STRING)
            USING CSV
            OPTIONS(PATH='data/flight_data/csv/departuredelays.csv',HEADER=True)""")

spark.sql("SELECT * FROM us_flight_delays_tbl").show(5)

print(spark.catalog.listTables())

#create a unmanaged table using DataFrame API

flight_df=(
    spark.read.format("csv")
    .option("header","True")
    .schema("date STRING,delay INT,distance INT,origin STRING,destination STRING")
    .load("data/flight_data/csv/departuredelays.csv")
)

(
    flight_df.write
    .option("path","/tmp/data/us_flight_data")
    .saveAsTable("us_flight_delays_tbl_unmanaged")
)

print(spark.catalog.listTables())

spark.stop()