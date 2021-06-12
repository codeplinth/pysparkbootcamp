from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, date_sub, lit,expr,col, regexp_extract, regexp_replace,locate

spark=(
    SparkSession
    .builder
    .appName("eg3_retial_data")
    .getOrCreate()
)

df=(
    spark.read.csv(
    path="data/retail-data/by-day/2010-12-01.csv",
    header=True,
    inferSchema=True
    )
)

# alternative way
# df=(
#     spark.read.format("csv")
#     .option("header","true")
#     .option("inferSchema","true")
#     .load("data/retail-data/by-day/2010-12-01.csv")
# )

df.printSchema()
df.show()
df.createOrReplaceTempView("dfTable")

df.select(lit(5),lit("five"),lit(5.0)).show()

(
    df.where("InvoiceNo = 536365")
    .select("InvoiceNo","Description").show(5)
)

(
    df.where(expr("StockCode in ('DOT') and ( UnitPrice>600 or instr(Description,'POSTAGE') >0 )")).show()
)

(
    df.withColumn("isExpensive",expr("StockCode in ('DOT') and ( UnitPrice>600 or instr(Description,'POSTAGE') >0 )"))
    .where("isExpensive")
    .select("*").show()

)

df.selectExpr("initcap(Description)").show(5,truncate=False)
df.selectExpr("lower(Description)","upper(Description)").show(5)

(
    df.selectExpr("ltrim('  HELLO   ') as ltrim",
    "rtrim('  HELLO   ') as rtrim",
    "trim('  HELLO   ') as trim",
    "lpad('HELLO',3,'X') as lp",
    "rpad('HELLO',10,'X') as rp"
    ).show(2)
)

df.selectExpr("Description","regexp_replace(Description,'RED|BLUE|WHITE|BLACK','COLOR') as New_Description").show()

df.selectExpr("Description","translate(Description,'LEET','1337') as New_Description").show()

dateDF=spark.range(10).withColumn("today",current_date()).withColumn("now",current_timestamp())
#dateDF=spark.range(10).withColumn("today",expr("current_date()")).withColumn("now",expr("current_timestamp()"))

dateDF.printSchema()

dateDF.select(date_sub(col("today"),5),date_add(col("today"),5)).show(1)

dateDF.selectExpr("date_sub(today,5)","date_add(today,5)").show(1)

spark.stop()