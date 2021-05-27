from pyspark.sql import SparkSession

data1 = [("Ashok", "Sales", 3000),
    ("Sanjay", "Sales", 4600),
    ("Saif", "Sales", 4100),
    ("Anuj", "Finance", 3300),
    ("Ashok", "Sales", 3000)
    ]

data2 = [("Scott", "Finance", 3300),
    ("Jai", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Sanjay", "Sales", 4100)
    ]

columns1=["name","dept","salary"]
columns2=["emp_name","emp_dept","emp_salary"]

spark=(SparkSession
        .builder
        .master("local[*]")
        .appName("using_union")
        .getOrCreate())

df1=spark.createDataFrame(data=data1,schema=columns1)
df1.printSchema()
df1.show(truncate=False)

#union resolves by position
df2=spark.createDataFrame(data=data2,schema=columns2)
df2.printSchema()
df2.show(truncate=False)

df_union=df1.union(df2)
df_union.printSchema()
df_union.show(truncate=False)

#remove duplicates
df_union_distinct=df2.union(df1).distinct()
df_union_distinct.printSchema()
df_union_distinct.show(truncate=False)

spark.stop()