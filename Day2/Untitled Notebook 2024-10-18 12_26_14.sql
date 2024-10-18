-- Databricks notebook source
select * from customers;

-- COMMAND ----------

describe history customers;

-- COMMAND ----------

delete from customers where customer_id=1

-- COMMAND ----------

describe history customers

-- COMMAND ----------


