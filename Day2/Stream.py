# Databricks notebook source
schema="Id int,Name string,Gender string,Country string,Date string"

# COMMAND ----------

(spark
 .readStream
 .schema(schema)
 .csv("/Volumes/sony_dev/bronze/stream_in/",header=True)
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint")
    .trigger(processingTime='10 seconds')
    .table("sony_dev.bronze.stream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sony_dev.bronze.stream

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation")
 .load("/Volumes/sony_dev/bronze/stream_in/")
 .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint")
    .table("sony_dev.bronze.autoloader"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sony_dev.bronze.autoloader;
