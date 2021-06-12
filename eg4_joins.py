from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark=(
    SparkSession
    .builder
    .appName("eg_joins")
    .getOrCreate()
)

person=(
    spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])
]).toDF("id","name","graduate_program","spark_status")
)

graduateProgram=(
    spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")
]).toDF("id", "degree", "department", "school")
)

sparkStatus=(
    spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")
]).toDF("id", "status")
)

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

#joinExpression=person.graduate_program == graduateProgram.id
joinExpression=expr("graduate_program = id")

person.withColumnRenamed("id","personId").join(graduateProgram,joinExpression,"inner").show()
#person.join(graduateProgram,joinExpression,"inner").explain(extended=True)

#person.join(graduateProgram,joinExpression,"outer").show()

# (
#     person.withColumnRenamed("id","personId")
#     .join(sparkStatus,expr("array_contains(spark_status,id)")).show()
# )

spark.stop()