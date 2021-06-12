from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

spark=(
    SparkSession
    .builder
    .master("local[*]")
    .appName("eg_departuredelays")
    .getOrCreate()
)

schema=StructType([
    StructField("date",StringType(),True),
    StructField("delay",IntegerType(),True),
    StructField("distance",IntegerType(),True),
    StructField("origin",StringType(),True),
    StructField("destination",StringType(),True)
])

path="data/departuredelays.csv"

df=(
    spark.read.format("csv")
    .schema(schema)
    .option("header","True")
    .load(path)
)

df.printSchema()
df.createOrReplaceTempView("us_delay_fligts_tbl")

spark.sql("""SELECT distance,origin,destination
FROM us_delay_fligts_tbl 
WHERE distance >= 1000 ORDER BY distance DESC""").show(10)

spark.sql("""SELECT date,delay,origin,destination
FROM us_delay_fligts_tbl
WHERE delay >120 AND origin='SFO' AND destination='ORD' ORDER BY delay DESC""").show(10,truncate=False)

spark.sql("""SELECT MONTH(TO_TIMESTAMP(date,'MMddHHmm'))||'-'||DAY(TO_TIMESTAMP(date,'MMddHHmm')) AS DT,delay
FROM us_delay_fligts_tbl ORDER BY delay DESC""").show(10,truncate=False)

spark.sql("""SELECT origin,destination,delay,
CASE 
WHEN delay >= 360 THEN 'Very Long Delay'
WHEN delay >= 120 AND delay < 360 THEN 'Long Delay'
WHEN delay >= 60 AND delay < 120 THEN 'Short Delay'
WHEN delay > 0  AND delay < 60 THEN 'Very Short Delay'
WHEN delay = 0 THEN 'No Delay'
ELSE 'Early Arrival'
END AS flight_delays
FROM us_delay_fligts_tbl ORDER BY origin,delay DESC""").show(10,truncate=False)

spark.stop()