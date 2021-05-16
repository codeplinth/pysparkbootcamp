from pyspark.sql import SparkSession

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
.appName("read_write_parquet_files")
.getOrCreate()
)

df=spark.createDataFrame(data=data,schema=columns)
df.printSchema()
df.show(truncate=False)

(df.write.format("parquet")
.mode("overwrite")
.option("compression","snappy")
.save("/tmp/data/parquet/emp.parquet"))

par_df=(spark.read.format("parquet")
.load("/tmp/data/parquet/emp.parquet"))

par_df.createOrReplaceTempView("employee")
spark.sql("SELECT * FROM employee WHERE salary >= 65000").show(truncate=False)

spark.stop()