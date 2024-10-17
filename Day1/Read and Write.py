# Databricks notebook source
df=spark.read.csv("/Volumes/sameer_databricks/default/raw/sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df_customer=spark.read.json("/Volumes/sameer_databricks/default/raw/customers.json")
df_customer.display()

# COMMAND ----------

df_customer.write.saveAsTable("customer")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

