# Reading and Writing Tables from/in SQL using PySpark :
# (Source : https://www.sqlrelease.com/read-and-write-data-to-sql-server-from-spark-using-pyspark)
# 1. Reading Tables from PySpark -

  from pyspark import SparkContext,SparkConf
  from pyspark.sql import SparkSession

  # Create spark conf object
  conf=SparkConf()
  conf.setMaster('local').setAppName('My App')

  # Create spark session and context
  sc=SparkContext.getOrCreate(conf=conf)
  spark=SparkSession(sc)

  # Or can just do the following in place of lines 9 - 15
  spark=SparkSession().builder.master('local').appName('My App').getOrCreate()

  # Set Variables to be used in connection
  dbname= 'TestDB'
  table='dbo.tbl_spark_df'
  user='test'
  password='********'

  # Read table into Spark DataFrame
  jdbcDF=spark.read.format('jdbc') \
        .option("url",f"jdbc:sqlserver://localhost:1433;databaseName={dbname};") \
        .option("dbtable",table) \
        .option("user",user) \
        .option("password",password) \
        .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()

 # Show first 20 entries
 jdbcDF.show()

 # 2. Writing into a SQL database
 # Reading a csv file into a df
 df=spark.read.csv("path_of_the_file")
 df.write.mode('overwrite').format('jdbc') \
    .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()
