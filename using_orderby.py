from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import col

data=[
    ("Harsh","IT","UP",60000,34,1000),
    ("Prateek","IT","DL",75000,28,12000),
    ("Ashok","FIN","HR",50000,50,5000),
    ("Manish","IT","UK",90000,32,15000),
    ("Anshika","IT","UP",65000,21,20000)
]

schema = StructType([
    StructField("name",StringType(),False),
    StructField("dept",StringType(),False),
    StructField("state",StringType(),False),
    StructField("salary",LongType(),False),
    StructField("age",IntegerType(),False),
    StructField("bonus",LongType(),False)
])

spark=(
    SparkSession
    .builder
    .appName("using_orderby")
    .getOrCreate()
)

df=spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

df.sort(col("dept"),col("state")).show(truncate=False)
df.sort(col("dept").asc(),col("state").asc()).show(truncate=False)
df.orderBy(col("dept").asc(),col("state").asc()).show(truncate=False)

df.sort(df.dept.desc(),df.state.desc()).show(truncate=False)
df.sort(col("dept").desc(),col("state").desc()).show(truncate=False)
df.orderBy(col("dept").desc(),col("state").desc()).show(truncate=False)

df.createOrReplaceTempView("employee")

spark.sql("""SELECT * FROM employee
            ORDER BY state,dept 
            """).show(truncate=False)

spark.sql("""SELECT * FROM employee
            ORDER BY state DESC ,dept DESC 
            """).show(truncate=False)

spark.stop()