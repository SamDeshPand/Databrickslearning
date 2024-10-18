# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists sameer_databricks.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table sameer_databricks.gold.orders_weekofyear as 
# MAGIC select week_of_year,count(*) as count from sameer_databricks.default.order_dates group by week_of_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sameer_databricks.gold.orders_weekofyear
