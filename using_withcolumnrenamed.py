from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col

data=[(("Prateek","","Kapila"),"1991-04-01","M",3000),
  (("Satyajeet","",""),"2000-05-19","M",4000),
  (("Rajeev","Kumar","Jha"),"1978-09-05","M",4000),
  (("Anshika","","Srivastava"),"2000-12-01","F",4000),
  (("Yogita","","Bhardwaj"),"1990-02-17","F",-1)
]

schema=StructType([
    StructField("name",StructType([
        StructField("firstname",StringType(),True),
        StructField("middlename",StringType(),True),
        StructField("lastname",StringType(),True)
    ]),True),
    StructField("dob",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True)
])

spark=(SparkSession
        .builder
        .appName("using_withcolumnrenamed")
        .getOrCreate())

df=spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)
df.printSchema()

df.withColumnRenamed("dob","dateofbirth").printSchema()

df2=(df.withColumnRenamed("dob","dateofbirth")
        .withColumnRenamed("salary","sal_amount"))

df2.printSchema()

schema_name=StructType([
    StructField("fname",StringType(),True),
    StructField("mname",StringType(),True),
    StructField("lname",StringType(),True)
])

df.select(col("name").cast(schema_name),col("dob"),col("gender"),col("salary")).printSchema()
df.select(col("name").cast(schema_name),col("dob"),col("gender"),col("salary")).show(truncate=False)

df3=(df.select(col("name.firstname").alias("fname"),
col("name.middlename").alias("mname"),
col("name.lastname").alias("lname"),
col("gender"),col("dob"),col("salary")
))

df3.printSchema()
df3.show(truncate=False)

columns=["col1","col2","col3","col4"]

df.toDF(*columns).printSchema()

spark.stop()

