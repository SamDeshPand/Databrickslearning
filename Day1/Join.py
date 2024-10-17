# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customer=spark.table("customers")

# COMMAND ----------

df_joined=df_sales.join(df_customer,"customer_id","inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_customer.filter("customer_id=2").display()

# COMMAND ----------

df_customer.filter("customer_id=2").display()

# COMMAND ----------

from pyspark.sql.functions import col
df_customer.where(col("customer_id")==2).display()

# COMMAND ----------

df_customer.where("customer_id>2 or customer_city='New MichealView'").display()

# COMMAND ----------

df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers inner join sales on customers.customer_id=sales.customer_id
