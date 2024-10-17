# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE table customers as 
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/sameer_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/sameer_databricks/default/raw/sales.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table product as 
# MAGIC select *,current_timestamp() as ingestion_data from json.`/Volumes/sameer_databricks/default/raw/products.json`
