from pyspark.sql import SparkSession

emp_data=[
    ("Harsh",1,"UP",60000,34,1000),
    ("Prateek",1,"DL",75000,28,12000),
    ("Ashok",2,"HR",50000,50,5000),
    ("Manish",1,"UK",90000,32,15000),
    ("Anshika",1,"UP",65000,21,20000),
    ("Havasingh",4,"RJ",55000,55,5500)
]

emp_columns=["name","dept_id","state","salary","age","bonus"]

dept_data=[
    (1,"IT"),
    (2,"FIN"),
    (3,"HR"),
    (4,"AD")
]

dept_columns=["dept_id","dept_cd"]

spark=(
    SparkSession
    .builder
    .appName("using_joins")
    .getOrCreate()
)

emp_df=spark.createDataFrame(data=emp_data,schema=emp_columns)
emp_df.printSchema()
emp_df.show(truncate=False)

dept_df=spark.createDataFrame(data=dept_data,schema=dept_columns)
dept_df.printSchema()
dept_df.show(truncate=False)

emp_df.join(dept_df,emp_df.dept_id == dept_df.dept_id,"inner").explain(extended=True)
emp_df.join(dept_df,emp_df.dept_id == dept_df.dept_id,"inner").show(truncate=False)

spark.stop()
