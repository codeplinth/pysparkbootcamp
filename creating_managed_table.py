from pyspark.sql import SparkSession

spark=(
    SparkSession
    .builder
    .appName("creating_managed_tables")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

#default database
print(spark.catalog.listDatabases())

spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
spark.sql("USE learn_spark_db")

print(spark.catalog.listDatabases())

#creating a managed table using SQL
spark.sql("""CREATE TABLE IF NOT EXISTS managed_us_delay_flights_tbl(
                date STRING,
                delay INT,
                distance INT,
                origin STRING,
                destination STRING)""")

path="data/departuredelays.csv"
schema="date STRING,delay INT,distance INT,origin STRING,destination STRING"

#read the data into a DataFrame
df=(
    spark.read.format("csv")
    .option("header","True")
    .schema(schema)
    .load(path)
)

#register a temp view
df.createOrReplaceTempView("departuredelays_data")

#Insert data into table using temp view
spark.sql("INSERT INTO managed_us_delay_flights_tbl SELECT * FROM departuredelays_data")
spark.sql("SELECT * FROM managed_us_delay_flights_tbl").show(10)

#creating a managed table using DataFrame API

flight_df=(
    spark.read.format("csv")
    .option("header","True")
    .schema(schema)
    .load(path)
)

flight_df.write.saveAsTable("managed_us_delay_flights_tbl_using_df_api",mode="overwrite")

print(spark.catalog.listTables())

#drop database
#spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE;")

spark.stop()