from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

spark=(
    SparkSession
    .builder
    .appName("read_write_csv_files")
    .getOrCreate()
)

flightSchema=StructType([
    StructField("DEST_COUNTRY_NAME",LongType(),True),
    StructField("ORIGIN_COUNTRY_NAME",LongType(),True),
    StructField("count",IntegerType(),True)
])

df=(
    spark.read.format("csv")
    .option("header","true")
    .option("mode","FAILFAST")
    .option("inferSchema","true")
    #.schema(flightSchema)
    .load("data/flight_data/csv/2010-summary.csv")
)

df.printSchema()
df.show(truncate=False)

(
    df.write.format("csv")
    .mode("overwrite")
    .option("sep","\t")
    .save("tmp/my_tsv")
)

spark.stop()
