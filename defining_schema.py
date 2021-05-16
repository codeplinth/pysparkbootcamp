from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,MapType

spark = (SparkSession
        .builder
        .master("local")
        .appName("defining_schema")
        .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
#print(spark.sparkContext.uiWebUrl)

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
    ]

schema = StructType([
        StructField("firstname",StringType(),True),
        StructField("middlename",StringType(),True),
        StructField("lastname",StringType(),True),
        StructField("id",StringType(),True),
        StructField("gender",StringType(),True),
        StructField("salary",IntegerType(),True)
    ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

structuredData = [(("James","","Smith"),"36636","M",3000),
    (("Michael","Rose",""),"40288","M",4000),
    (("Robert","","Williams"),"42114","M",4000),
    (("Maria","Anne","Jones"),"39192","F",4000),
    (("Jen","Mary","Brown"),"","F",-1)
    ]

structuredSchema = StructType([
    StructField("name",
        StructType([
            StructField("firstname",StringType(),True),
            StructField("middlename",StringType(),True),
            StructField("lastname",StringType(),True)
        ]),True
    ),
    StructField("id",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True)
])

df2 = spark.createDataFrame(data=structuredData,schema=structuredSchema)
df2.printSchema()
df2.show(truncate=False)

arrayStructuredSchema = StructType([
    StructField("name",StructType([
        StructField("firstname",StringType(),True),
        StructField("middlename",StringType(),True),
        StructField("lastname",StringType(),True)
    ])),
    StructField("hobbies",ArrayType(StringType()),True),
    StructField("properties",MapType(StringType(),StringType()),True)
])

arrayStructuredData = [(("James","","Smith"),["cricket","badminton","football"],{"noida":"Sector 59","delhi":"NSP"}),
    (("Michael","Rose",""),["table tennis","hockey"],{"etawah":"civil lines"}),
    (("Robert","","Williams"),["basketball","table tennis","hockey"],{"kanpur":"mall road"}),
    (("Maria","Anne","Jones"),["tennis"],{"noida":"Atta"}),
    (("Jen","Mary","Brown"),[],{})
    ]

df3= spark.createDataFrame(data=arrayStructuredData,schema=arrayStructuredSchema)
df3.printSchema()
print(df3.schema.simpleString())
df3.show(truncate=False)

x = input("Press any key")
spark.stop()
