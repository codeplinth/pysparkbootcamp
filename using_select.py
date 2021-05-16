from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

data=[("Harsh","Tripathi","India","UP"),
    ("Manish","Kumar","India","UK"),
    ("Prateek","Kapila","India","DL"),
    ("Anshika","Srivastava","India","UP")
]

schema=StructType([
    StructField("firstname",StringType(),False),
    StructField("lastname",StringType(),False),
    StructField("country",StringType(),False),
    StructField("state",StringType(),False)
])

spark=(SparkSession
    .builder
    .master("local[*]")
    .appName("using_select")
    .getOrCreate()
    )

df=spark.createDataFrame(data=data,schema=schema)
df.printSchema()
columns=df.columns

df.select("firstname","lastname").show()
#using DataFrame object name
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()

#using col function
df.select(col("firstname")).show()

#select all columns
df.select("*").show()
df.select(*columns).show()
df.select(df.columns).show()
df.select([col for col in df.columns]).show()

#select columns by index
#select first 3 columns and top 2 rows
df.select(df.columns[:3]).show(2)
#select columns 1 to 3 and top 2 rows
df.select(df.columns[1:3]).show(2)

nested_data=[
    (("Harsh",None,"Tripathi"),"UP","M"),
    (("Kapil",None,"Khurana"),"HR","M"),
    (("Rajeev","Kumar","Jha"),"BR","M"),
    (("Anshika",None,"Srivastava"),"UP","F"),
    (("Prateek","","Kapila"),"DL","M")
    ]

nested_schema=StructType([
    StructField("name",StructType([
        StructField("firstname",StringType(),False),
        StructField("middlename",StringType(),True),
        StructField("lastname",StringType(),False),
    ]),False),
    StructField("state",StringType(),False),
    StructField("gender",StringType(),False)
])

nested_df=spark.createDataFrame(data=nested_data,schema=nested_schema)
nested_df.select(col("name")).show()
nested_df.select(col("name.firstname"),col("name.lastname")).show()
nested_df.select(col("name.*")).show(truncate=False)

spark.stop()
