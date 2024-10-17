# Databricks notebook source


# COMMAND ----------

# MAGIC %md 
# MAGIC # SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

spark

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema=["id","name","age"]  #schema="id int,name string,age int"
df=spark.createDataFrame(data,schema)  
df.display()

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("id","age").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

##Can also save it df1=df.select("*")
df.select(col("id").alias("emp_id")).display()


# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display()

# COMMAND ----------

df.withColumn("age",current_date()).display()

# COMMAND ----------

df.display()
