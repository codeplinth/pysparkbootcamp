from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark=(
    SparkSession
    .builder
    .config("spark.jars.packages","org.xerial:sqlite-jdbc:3.34.0")
    .appName("using_sqlit_db")
    .getOrCreate()
)

df=(
    spark.read.format("jdbc")
    .option("driver", "org.sqlite.JDBC")
    .option("url", "jdbc:sqlite:data/flight_data/jdbc/my-sqlite.db")
    .option("dbtable", "flight_info")
    .load()
)

df.printSchema()
df.show(10,truncate=False)

df.select("DEST_COUNTRY_NAME").distinct().show()
df.select("DEST_COUNTRY_NAME").distinct().explain(extended=True)

(
    df.where(expr("DEST_COUNTRY_NAME in ('Anguilla','Sweden')"))
    .explain(extended=True)
)

#query pushdown
pushdownQuery="""(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info"""

dfpushdownQuery=(
    spark.read.format("jdbc")
    .option("driver", "org.sqlite.JDBC")
    .option("url", "jdbc:sqlite:data/flight_data/jdbc/my-sqlite.db")
    .option("dbtable", pushdownQuery)
    .load()
)

dfpushdownQuery.show()

dfpushdownQuery.explain(extended=True)

spark.stop()