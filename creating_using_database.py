from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("reading_managed_table")
    .enableHiveSupport()
    .getOrCreate()
)

print(spark.catalog.listDatabases())

spark.sql("create database if not exists learn_spark_db")

print(spark.catalog.listDatabases())

spark.sql("use learn_spark_db")

print(spark.catalog.listTables())

spark.sql("""
create table if not exists managed_us_delay_flights_tbl(
    date timestamp,
    delay int,
    distance int,
    origin string,
    destination string
)
""")

print(spark.catalog.listTables())

spark.sql("select * from managed_us_delay_flights_tbl").show()

csv_file_path="data/flight_data/csv/departuredelays.csv"

schema=StructType([
    StructField("date",TimestampType(),True),
    StructField("delay",IntegerType(),True),
    StructField("distance",IntegerType(),True),
    StructField("origin",StringType(),True),
    StructField("destination",StringType(),True)
])

df=(
    spark.read.csv(
            path=csv_file_path,
            schema=schema,
            header=True,
            timestampFormat="MMddHHmm"
            )
)

df.createOrReplaceTempView("us_delay_flights_data")

spark.sql("""
insert into managed_us_delay_flights_tbl select * from us_delay_flights_data
""")

spark.sql("select * from managed_us_delay_flights_tbl").show()

df.write.saveAsTable("managed_us_delay_flights_tbl_using_df",mode="overwrite")

spark.sql("""
create table if not exists us_delay_flights_tbl(
        date timestamp,
        delay int,
        distance int,
        origin string,
        destination string
)
using csv
options(path='data/flight_data/csv/departuredelays.csv',header=True,timestampFormat='MMddHHmm',mode='overwrite')
""")

spark.sql("select * from us_delay_flights_tbl").show()

print(spark.catalog.listTables())

df.write.option("path","tmp/data/flight_data/").saveAsTable("us_delay_flights_tbl_using_df",mode="overwrite")

print(spark.catalog.listTables())

spark.stop()