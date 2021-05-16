from pyspark.sql import Row,SparkSession

#create a Row objet
row = Row("James",40)
print(row[0]+","+str(row[1]))

#using Named arguments
row = Row(name="Alice",age=11)
print(row.name)
print(row.age)

#create a custom class from Row
Person = Row("name","age")
p1 = Person("James",40)
p2 = Person("Alice",11)
print(p1.name+","+p2.name)

spark = (SparkSession
        .builder
        .master("local")
        .appName("using_row_object")
        .getOrCreate()
        )

data = [
    Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")
]

#create RDD from Row objects
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

#collect returns Row
collData = rdd.collect()
for rows in collData:
    print(rows.name+" , "+str(rows.lang))

#create DataFrame from Row objects
df = spark.createDataFrame(data)
df.printSchema()
df.show(truncate=False)

#changing column names
columns = ["name","languages","currentstate"]
df2 = spark.createDataFrame(data).toDF(*columns)
df2.printSchema()

#create nested Struct using Row object
data = [
    Row(name="James",prop=Row(hair="black",eye="blue")),
    Row(name="Annie",prop=Row(hair="grey",eye="black"))
]

df3 = spark.createDataFrame(data)
df3.printSchema()
df3.show()

spark.stop()
