# Databricks notebook source
# MAGIC %md
# MAGIC ### 180. Consecutive Numbers

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import *


# Define Schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("num", IntegerType(), True)
])

# Create Data
data = [(1, 1), (2, 1), (3, 1), (4, 2), (5, 1), (6, 2), (7, 2)]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

consecutive_df=df.groupBy(col("num")).count()
result_df=consecutive_df.filter((col("count")>3).alias("ConsecutiveNums")).select("num").display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType



# Define Schema for Customers Table
customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Customers Data
customers_data = [
    (1, "Joe"),
    (2, "Henry"),
    (3, "Sam"),
    (4, "Max")
]

# Create Customers DataFrame
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Define Schema for Orders Table
orders_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerId", IntegerType(), True)
])

# Orders Data
orders_data = [
    (1, 3),
    (2, 1)
]

# Create Orders DataFrame
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Show DataFrames
print("Customers Table:")
customers_df.show()

print("Orders Table:")
orders_df.show()


# COMMAND ----------

joined_df=customers_df.join(orders_df,customers_df["id"] == orders_df["customerId"],"left")
filtered_df = joined_df.filter(col("customerId").isNull())

display(filtered_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define Schema for Employee Table
employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("departmentId", IntegerType(), True)
])

# Employee Data
employee_data = [
    (1, "Joe", 85000, 1),
    (2, "Henry", 80000, 2),
    (3, "Sam", 60000, 2),
    (4, "Max", 90000, 1),
    (5, "Janet", 69000, 1),
    (6, "Randy", 85000, 1),
    (7, "Will", 70000, 1)
]

# Create Employee DataFrame
employee_df = spark.createDataFrame(employee_data, schema=employee_schema)

# Define Schema for Department Table
department_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Department Data
department_data = [
    (1, "IT"),
    (2, "Sales")
]

# Create Department DataFrame
department_df = spark.createDataFrame(department_data, schema=department_schema)



# COMMAND ----------

# DBTITLE 1,employees who are high earners in each of the departments.
window_spec=Window.partitionBy("departmentId").orderBy(col("salary").desc())
joined_df=employee_df.join(department_df,employee_df["departmentId"]==department_df["id"],"inner")

ranked_df=joined_df.withColumn("rank_sal",dense_rank().over(window_spec))

display(ranked_df)




# COMMAND ----------

result_df=ranked_df.filter(ranked_df["rank_sal"] <= 3).select(department_df["name"],employee_df["name"],employee_df["salary"]).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType



# Define Schema for Activity Table
activity_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", DateType(), True),  # Keeping as String; can be converted to DateType if needed
    StructField("games_played", IntegerType(), True)
])

# Activity Data
from datetime import date

activity_data = [
    (1, 2, date(2016, 3, 1), 5),
    (1, 2, date(2016, 5, 2), 6),
    (2, 3, date(2017, 6, 25), 1),
    (3, 1, date(2016, 3, 2), 0),
    (3, 4, date(2018, 7, 3), 5)
]


# Create Activity DataFrame
activity_df = spark.createDataFrame(activity_data, schema=activity_schema)

# Show DataFrame
activity_df.show()


# COMMAND ----------

result_df=df.groupBy(col("player_id")).agg(min(col("event_date"))).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Employee Table Data
employee_data = [
    (3, "Brad", None, 4000),
    (1, "John", 3, 1000),
    (2, "Dan", 3, 2000),
    (4, "Thomas", 3, 4000)
]

employee_schema = StructType([
    StructField("empId", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("supervisor", IntegerType(), True),
    StructField("salary", IntegerType(), False)
])

employee_df = spark.createDataFrame(employee_data, schema=employee_schema)

# Bonus Table Data
bonus_data = [
    (2, 500),
    (4, 2000)
]

bonus_schema = StructType([
    StructField("empId", IntegerType(), False),
    StructField("bonus", IntegerType(), True)
])

bonus_df = spark.createDataFrame(bonus_data, schema=bonus_schema)

# Show DataFrames
employee_df.show()
bonus_df.show()


# COMMAND ----------

joined_df=employee_df.join(bonus_df,employee_df["empId"]==bonus_df["empId"],"left")

result_df = joined_df.filter(when(col("bonus").isNull(), True).when(col("bonus") <= 1000, True).otherwise(False))

display(result_df)

# COMMAND ----------


employee_data=[
    (1,"alice",5000),
    (2,"max",2000)
]

employee_schema=(["id","name","sal"])

employee_df=spark.createDataFrame(employee_data,employee_schema)


# COMMAND ----------

employee_df.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Helper to convert date string to datetime.date
def parse_date(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

# Sample data with parsed dates
data = [
    (7369, 'smith', 'clerk', 7566, parse_date('1980-12-17'), 800, 0, 20),
    (7499, 'Allen', 'Salesman', 7698, parse_date('1981-02-20'), 1600, 300, 30),
    (7521, 'Ward', 'Salesman', 7698, parse_date('1981-02-22'), 1250, 500, 30),
    (7566, 'Jones', 'Manager', 7839, parse_date('1981-04-02'), 2975, None, 20),
    (7698, 'Blake', 'Manager', 7839, parse_date('1981-05-01'), 2850, None, 30),
    (7782, 'Clark', 'Manager', 7839, parse_date('1981-06-09'), 2450, None, 10),
    (7788, 'Scott', 'Analyst', 7566, parse_date('1987-04-19'), 3000, None, 20),
    (7839, 'King', 'President', None, parse_date('1981-11-17'), 5000, None, 10),
    (7844, 'Turner', 'Salesman', 7698, parse_date('1981-09-08'), 1500, 0, 30),
    (7900, 'James', 'Clerk', 7698, parse_date('1981-12-03'), 950, None, 30),
    (7902, 'Ford', 'Analyst', 7566, parse_date('1981-12-03'), 3000, None, 20),
    (7934, 'Miller', 'Clerk', 7782, parse_date('1982-01-23'), 1300, None, 10)
]

# Define schema
schema = StructType([
    StructField("empno", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("job", StringType(), True),
    StructField("mgr", IntegerType(), True),
    StructField("hiredate", DateType(), True),
    StructField("sal", IntegerType(), True),
    StructField("comm", IntegerType(), True),
    StructField("deptno", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import *
max_sal_df=df.filter(col("job")=="Manager").agg(max(col('sal')).alias("Maximum sal")).display()

# COMMAND ----------

no_of_Emp_df=df.filter((col("deptno")== 20) & (col('sal')>1500)).count()

# COMMAND ----------

display(no_of_Emp_df)

# COMMAND ----------

no_of_emp_dept=df.groupBy(col("deptno")).count().orderBy(col("deptno").desc())
display(no_of_emp_dept)

# COMMAND ----------

max_sal_job=df.groupBy(col("job")).agg(max("sal")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC NUMBER OF EMPLOYEES WORKING IN EACH 
# MAGIC DEPARTEMENT EXCEPT PRESIDENT.

# COMMAND ----------

no_of_employees_not_pre=df.filter(col("job")!="president").groupBy(col("deptno")).count().withColumnRenamed("count", "num_of_employees")
display(no_of_employees_not_pre)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TOTAL SALARY NEEDED TO PAY ALL THE 
# MAGIC ### EMPLOYEES IN EACH JOB

# COMMAND ----------

sum_sal_df=df.groupBy(col("job")).agg(max(col("sal"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### .WAQTD AVG SALARY NEEDED TO PAY ALL THE 
# MAGIC ### EMPLOYEES IN EACH DEPARTMENT EXCLUDING THE 
# MAGIC ### EMPLOYEES OF DEPTNO 20.

# COMMAND ----------

df1=df.filter(col("deptno")!=20).groupBy("deptno").agg(avg(col("sal"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WAQDTD TOTAL SALARY NEEDED TO PAY AND NUMBER 
# MAGIC ### OF SALESMANS IN EACH DEPT.

# COMMAND ----------

df2=df.filter(col("job")=="Salesman").groupBy(col("deptno")).agg(sum(col("sal")))
display(df2)


# COMMAND ----------

no_emp_avg_count=df.filter(col("sal")>2000).groupBy("deptno").agg(count("*").alias("No_of_emp"),avg("sal").alias("average salary")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### NAME OF THE EMPLOYEES EARNING MORE THAN 
# MAGIC ### Allen

# COMMAND ----------

adams_sal=df.filter(col("ename")=='Allen').select('sal').collect()[0][0]
emp_df=df.filter(col("sal")>adams_sal).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  all the details of the employees working as salesman in the 
# MAGIC ### dept 20 and earning commission more than Smith and hired after 
# MAGIC ### KING .

# COMMAND ----------

salesman_df=df.filter((col('job')=='Salesman')&(col('deptno')==30)).display()

# COMMAND ----------

