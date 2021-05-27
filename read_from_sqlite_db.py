from pyspark.sql import SparkSession

spark=(
    SparkSession
    .builder
    .config("spark.jars.packages","org.xerial:sqlite-jdbc:3.34.0")
    .appName("using_sqlit_db")
    .getOrCreate()
)

df=(
    spark.read.format("jdbc")
    .option("driver", "org.sqlite.JDBC")
    .option("url", "jdbc:sqlite:data/flight_data/jdbc/my-sqlite.db")
    .option("dbtable", "flight_info")
    .load()
)

df.printSchema()
df.show(10,truncate=False)

spark.stop()