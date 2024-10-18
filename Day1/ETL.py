# Databricks notebook source
v=dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run /Workspace/Users/sameerdeshpande51@gmail.com/Databrickslearning/Day1/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}order_dates.csv",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("order_dates")

# COMMAND ----------



# COMMAND ----------

df2=df1.withColumn("environment",lit(v))

# COMMAND ----------

df2.display()
