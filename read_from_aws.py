from pyspark.sql import SparkSession
 
spark = (
        SparkSession.builder
        .appName("schema_test")
        # .config("spark.jars.packages","com.amazonaws:aws-java-sdk:1.11.1026")
        # .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.4")
        # .config('fs.s3a.access.key',"AKIAUYWYBTX76YGDLX45")
        # .config('fs.s3a.secret.key', "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")
        .getOrCreate()
)

# df = spark.read.csv("s3a://buck-mumbai-hr/emp_data.txt", header = 'True', inferSchema = True)
 
# df.printSchema()

spark.stop()


#hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()

#hadoop_conf.set("fs.s3a.access.key", "AKIAUYWYBTX73AZOJQGB")
#hadoop_conf.set("fs.s3a.secret.key", "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")
#hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
#hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
#parqDF = spark.read.csv("s3a://bukmanishkumar247/Test_manish.csv")
#parqDF.printSchema()