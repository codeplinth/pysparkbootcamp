from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max

data=[
    ("Harsh","IT","UP",60000,34,1000),
    ("Prateek","IT","DL",75000,28,12000),
    ("Ashok","FIN","HR",50000,50,5000),
    ("Manish","IT","UK",90000,32,15000),
    ("Anshika","IT","UP",65000,21,20000)
]

columns=["name","dept","state","salary","age","bonus"]

spark=(SparkSession
        .builder
        .master("local[*]")
        .appName("using_groupby")
        .getOrCreate()
        )

df=spark.createDataFrame(data=data,schema=columns)
df.printSchema()
df.show(truncate=False)

df.groupBy(col("dept")).count().show(truncate=False)

df.groupBy("dept").sum("salary").show(truncate=False)

df.groupBy("dept","state").sum("salary","bonus").show(truncate=False)

(df.groupBy("dept","state")
    .agg(sum("salary").alias("sum_salary"),
        avg("salary").alias("avg_salary"),
        sum("bonus").alias("sum_bonus"),
        max("bonus").alias("max_bonus")
        ).show(truncate=False))

(df.groupBy(col("dept"),col("state"))
    .agg(sum(col("salary")).alias("sum_salary"),
        avg(col("salary")).alias("avg_salary"),
        sum(col("bonus")).alias("sum_bonus"),
        max(col("bonus")).alias("max_bonus")
        ).where(col("sum_bonus") > 5000).show(truncate=False))


spark.stop()
