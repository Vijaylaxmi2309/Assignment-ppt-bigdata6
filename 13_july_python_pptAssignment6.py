#!/usr/bin/env python
# coding: utf-8

# 1. Working with RDDs:
#    a) Write a Python program to create an RDD from a local data source.
#    b) Implement transformations and actions on the RDD to perform data processing tasks.
#    c) Analyze and manipulate data using RDD operations such as map, filter, reduce, or aggregate.
# 
# 2. Spark DataFrame Operations:
#    a) Write a Python program to load a CSV file into a Spark DataFrame.
#    b)Perform common DataFrame operations such as filtering, grouping, or joining.
#    c) Apply Spark SQL queries on the DataFrame to extract insights from the data.
# 
# 3. Spark Streaming:
#   a) Write a Python program to create a Spark Streaming application.
#    b) Configure the application to consume data from a streaming source (e.g., Kafka or a socket).
#    c) Implement streaming transformations and actions to process and analyze the incoming data stream.
# 
# 4. Spark SQL and Data Source Integration:
#    a) Write a Python program to connect Spark with a relational database (e.g., MySQL, PostgreSQL).
#    b)Perform SQL operations on the data stored in the database using Spark SQL.
#    c) Explore the integration capabilities of Spark with other data sources, such as Hadoop Distributed File System (HDFS) or Amazon S3.
# 
# 
# 
# 
# 

# In[1]:


pip install pyspark


# In[2]:


#1ANS
from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext("local", "RDD Example")

# Create an RDD from a local data source (list)
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
# Apply transformations and actions on the RDD
squared_rdd = rdd.map(lambda x: x ** 2)  # Map each element to its square
filtered_rdd = squared_rdd.filter(lambda x: x > 10)  # Filter elements greater than 10
sum_of_squared_values = filtered_rdd.reduce(lambda x, y: x + y)  # Calculate the sum

# Print the results
print("Squared RDD:", squared_rdd.collect())
print("Filtered RDD:", filtered_rdd.collect())
print("Sum of squared values:", sum_of_squared_values)


# In[3]:


#2ANS
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()

# Load a CSV file into a DataFrame
df = spark.read.csv("file.csv", header=True, inferSchema=True)
# Perform DataFrame operations
filtered_df = df.filter(df["age"] > 30)  # Filter rows where age > 30
grouped_df = df.groupBy("gender").count()  # Group by gender and count occurrences
joined_df = df.join(grouped_df, "gender", "inner")  # Join with grouped DataFrame

# Apply Spark SQL queries
df.createOrReplaceTempView("people")  # Create a temporary view for SQL queries
result = spark.sql("SELECT gender, AVG(age) FROM people GROUP BY gender")  # SQL query

# Show the results
filtered_df.show()
grouped_df.show()
joined_df.show()
result.show()


# In[4]:


#3ANS
from pyspark.streaming import StreamingContext

# Create a StreamingContext with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Configure the application to consume data from a streaming source
stream = ssc.socketTextStream("localhost", 9999)
# Perform streaming transformations and actions
word_counts = stream.flatMap(lambda line: line.split(" "))                    .map(lambda word: (word, 1))                    .reduceByKey(lambda a, b: a + b)

# Print the word counts
word_counts.pprint()

# Start the streaming computation
ssc.start()

# Wait for the streaming computation to finish
ssc.awaitTermination()


# In[5]:


#4ANS
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder     .appName("Spark SQL Example")     .config("spark.driver.extraClassPath", "jdbc_driver.jar")     .getOrCreate()

# Configure the connection to the database
url = "jdbc:postgresql://localhost:5432/mydatabase"
properties = {
    "user": "username",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Read data from the database into a DataFrame
df = spark.read.jdbc(url=url, table="mytable", properties=properties)
# Read data from HDFS into a DataFrame
hdfs_path = "hdfs://localhost:9000/data/file.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
# Read data from Amazon S3 into a DataFrame
s3_path = "s3a://my-bucket/data/file.csv"
df = spark.read.csv(s3_path, header=True, inferSchema=True)
# Register the DataFrame as a temporary view
df.createOrReplaceTempView("my_table")

# Perform SQL operations on the DataFrame
result = spark.sql("SELECT * FROM my_table WHERE column = 'value'")
result.show()


# In[ ]:




