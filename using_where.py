from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import array_contains, col

data=[
    (("Harsh","","Tripathi"),["Informatica","Python","Bash"],"UP","M"),
    (("Prateek","","Kapila"),["Informatica","SQL","Bash"],"DL","M"),
    (("Manish","Kumar","Sisodia"),["Informatica","SQL","Bash","Azure"],"UK","M"),
    (("Anshika","","Srivastava"),["Informatica","SQL","Bash"],"UP","F")
]

schema=StructType([
    StructField("name",StructType([
        StructField("firstname",StringType(),False),
        StructField("middlename",StringType(),False),
        StructField("lastname",StringType(),False)
    ])
    ,False),
    StructField("languages",ArrayType(StringType()),False),
    StructField("state",StringType(),False),
    StructField("gender",StringType(),False)
])

spark=(SparkSession
    .builder
    .master("local[*]")
    .appName("using_filter")
    .getOrCreate()
    )

df=spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

df.select("name").filter(df.state == "UP").show(truncate=False)
df.where("state == 'DL'").show(truncate=False)
df.where(col("state") != "UP").show(truncate=False)
df.where((col("state") == "UP") & (col("gender") == "F")).show(truncate=False)
df.where(array_contains(col("languages"),"Azure")).show(truncate=False)
df.where(col("name.lastname") == "Tripathi").show(truncate=False)

spark.stop()