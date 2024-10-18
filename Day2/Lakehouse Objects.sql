-- Databricks notebook source
use catalog sony_dev

-- COMMAND ----------

create schema bronze;

-- COMMAND ----------

use bronze;
create table emp(id int ,name string,age int)

-- COMMAND ----------

-- MAGIC %sql describe detail sony_dev.bronze.emp

-- COMMAND ----------

-- MAGIC %sql describe extended sony_dev.bronze.emp

-- COMMAND ----------

insert into emp values(4,'d',21),(5,'e',23),(6,'f',33);

-- COMMAND ----------

create view emp3 as select * from emp where id>3;

-- COMMAND ----------

create temp view emp as select * from emp where id=2

-- COMMAND ----------


show views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("/Volumes/sameer_databricks/default/raw/sales.csv",header=True,inferSchema=True)
-- MAGIC global_temp_view=df.createOrReplaceGlobalTempView("df")
-- MAGIC

-- COMMAND ----------


show views in global_temp

-- COMMAND ----------

--vaccum(7 days files will be there)
--unused stale files will be deleted
--vaccum sony_emp retain 0 hours
