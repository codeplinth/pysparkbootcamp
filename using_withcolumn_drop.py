from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import col,lit

data=[("Prateek","","Kapila","1991-04-01","M",3000),
  ("Satyajeet","","","2000-05-19","M",4000),
  ("Rajeev","Kumar","Jha","1978-09-05","M",4000),
  ("Anshika","","Srivastava","2000-12-01","F",4000),
  ("Yogita","","Bhardwaj","1990-02-17","F",-1)
]

columns=["firstname","middlename","lastname","dob","gender","salary"]

spark=(SparkSession
        .builder
        .appName("using_withcolumn")
        .getOrCreate())

df=spark.createDataFrame(data=data,schema=columns)
df.printSchema()

#change datatype of a column
df1=df.withColumn("salary",col("salary").cast("integer"))
df1.show(truncate=False)

#change the value of an existing column
df2=df.withColumn("salary",col("salary")*100)
df2.show(truncate=False)

#create a column from existing column
df3=df.withColumn("bonus",col("salary")*-1)
df3.show(truncate=False)

#add a new column
df4=df.withColumn("country",lit("India")).withColumn("tempcolumn",lit("Dummy"))
df4.printSchema()
df4.show(truncate=False)

df5=df4.drop("tempcolumn").printSchema()

spark.stop()