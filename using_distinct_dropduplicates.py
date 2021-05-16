from os import truncate
from pyspark.sql import SparkSession

data = [("Ashok", "Sales", 3000),
    ("Sanjay", "Sales", 4600),
    ("Saif", "Sales", 4100),
    ("Anuj", "Finance", 3300),
    ("Ashok", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jai", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Sanjay", "Sales", 4100)
    ]

schema=["employee_name","dept","salary"]

spark=(SparkSession
.builder
.appName("using_distinct_dropduplicates")
.getOrCreate()
)

df=spark.createDataFrame(data=data,schema=schema)

df.show(truncate=False)

df.distinct().show(truncate=False)
df.dropDuplicates().show(truncate=False)

df.dropDuplicates(["dept","salary"]).show(truncate=False)
spark.stop()

