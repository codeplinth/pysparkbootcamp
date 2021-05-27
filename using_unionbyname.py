from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

data1 = [("Ashok", "Sales", 3000),
    ("Sanjay", "Sales", 4600),
    ("Saif", "Sales", 4100),
    ("Anuj", "Finance", 3300),
    ("Ashok", "Sales", 3000)
    ]

data2 = [("Finance", "Scott", 3300),
    ("Finance", "Jai", 3900),
    ("Marketing", "Jeff", 3000),
    ("Marketing", "Kumar", 2000),
    ("Sales", "Sanjay", 4100)
    ]	

columns1=["name","dept","salary"]
columns2=["dept","name","salary"]

spark=(SparkSession
        .builder
        .master("local[*]")
        .appName("using_unionbyname")
        .getOrCreate())

df1=spark.createDataFrame(data=data1,schema=columns1)
df1.printSchema()
df1.show(truncate=False)

df2=spark.createDataFrame(data=data2,schema=columns2)
df2.printSchema()
df2.show(truncate=False)

df_un=df1.unionByName(df2)
df_un.show()

data3 = [("Scott", "Finance","UK"),
    ("Jai", "Finance","UP"),
    ("Jeff", "Marketing","HR" ),
    ("Kumar", "Marketing","DL"),
    ("Sanjay", "Sales", "BR")
    ]
	
data4 = [("Finance", "Scott", 3300),
    ("Finance", "Jai", 3900),
    ("Marketing", "Jeff", 3000),
    ("Marketing", "Kumar", 2000),
    ("Sales", "Sanjay", 4100)
    ]	

columns3=["name","dept","state"]
columns4=["dept","name","salary"]

df3=spark.createDataFrame(data=data3,schema=columns3)
df3.printSchema()
df3.show(truncate=False)

df4=spark.createDataFrame(data=data4,schema=columns4)
df4.printSchema()
df4.show(truncate=False)

for column in [column for column in df4.columns if column not in df3.columns]:
    df3=df3.withColumn(column,lit(None))

df3.show(truncate=False)

for column in [column for column in df3.columns if column not in df4.columns]:
    df4=df4.withColumn(column,lit(None))

df4.show(truncate=False)

df_union_new=df3.unionByName(df4)
df_union_new.printSchema()
df_union_new.show(truncate=False)

spark.stop()