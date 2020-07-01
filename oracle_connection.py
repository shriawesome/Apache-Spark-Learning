# Reading and Writing Tables from/in ORACLE using PySpark :
# (Source : https://dzone.com/articles/read-data-from-oracle-database-with-apache-spark)
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
  empDF=spark.read.format('jdbc') \
        .option("url",f"jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
        .option("dbtable",table) \
        .option("user",user) \
        .option("password",password) \
        .option("driver","oracle.jdbc.driver.OracleDriver").load()


  # Getting the result of the query in the df
  query="(select empno,ename,dname from emp, dept where emp.deptno = dept.deptno) emp"
  empDF=spark.read.format('jdbc') \
          .option("url",f"jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
          .option("dbtable",query) \
          .option("user",user) \
          .option("password",password) \
          .option("driver","oracle.jdbc.driver.OracleDriver").load()

 # Show first 20 entries
 jdbcDF.show()

 # 2. Writing into a ORACLE database
 # Reading a csv file into a df
 df=spark.read.csv("path_of_the_file")
 df.write.mode('overwrite').format('jdbc') \
    .option("url", f"jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .save()
