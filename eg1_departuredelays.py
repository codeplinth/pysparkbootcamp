from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType
from pyspark.sql.functions import expr,col,desc

spark=(
    SparkSession
    .builder
    .master("local[*]")
    .appName("eg1_departuredelays")
    .getOrCreate()
)

csv_file_path="data/flight_data/csv/departuredelays.csv"

schema=StructType([
    StructField("date",TimestampType(),True),
    StructField("delay",IntegerType(),True),
    StructField("distance",IntegerType(),True),
    StructField("origin",StringType(),True),
    StructField("destination",StringType(),True)
])

df=spark.read.csv(
    path=csv_file_path,
    schema=schema,
    timestampFormat="MMddHHmm",
    header=True
    )

df.createOrReplaceTempView("us_delay_flights_tbl")

df.printSchema()

df.show(truncate=False)

#df.where(expr("distance > 1000")).select("origin","destination","distance").orderBy(desc("distance")).show(5)

spark.sql("""
select origin,destination,distance from us_delay_flights_tbl where distance > 1000 order by distance desc limit 5
""").show()

#df.select("date","delay","distance","origin","destination").where(expr("delay > 120 and origin='SFO' and destination='ORD'")).orderBy("delay",ascending=False).show(10)

spark.sql("""
select date,delay,distance,origin,destination from us_delay_flights_tbl
where delay > 120 and origin='SFO' and destination='ORD'
order by delay desc """).show(10)

spark.sql("""
select origin,destination,delay,
case
    when delay > 360 then 'Very Long Delay'
    when delay > 120 and delay <= 360 then 'Long Delay'
    when delay > 60 and delay <= 120 then 'Short Delay'
    when delay > 0 and delay <= 60 then 'Very Short Delay'
    when delay = 0 then 'No Delay'
    else 'Early'
end as flight_delays
from us_delay_flights_tbl
order by origin,delay desc""").show(10)

# (
#     df.withColumn("flight_delays",expr("case when delay > 360 then 'Very Long Delay' when delay > 120 and delay <= 360 then 'Long Delay' when delay > 60 and delay <= 120 then 'Short Delay' when delay > 0 and delay <= 60 then 'Very Short Delay' when delay = 0 then 'No Delay' else 'Early' end"))
#     .select("origin","destination","delay","flight_delays")
#     .orderBy("origin",desc("delay")).show(10)
# )

spark.stop()