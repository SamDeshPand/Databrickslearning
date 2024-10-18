# Databricks notebook source
simpleData = ((1,"James", "Sales", 3000), \
    (2,"Michael", "Sales", 4600),  \
    (3,"Robert", "Sales", 4100),   \
    (4,"Maria", "Finance", 3000),  \
    (5,"James", "Sales", 3000),    \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100),\
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (None,None,None,None),\
    (None,None,None,None),\
    (None,None,None,None),\
    (None,"Robert",None,2000),\
     (11,"Jack", None, 4100), \
    (12,"Steve", "Sales", None)  
  )
 
columns= ["id","employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

df1=df.dropDuplicates(["id"])

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

df2.display()


# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df3=df2.fillna({"department":"finance","salary":4600})

# COMMAND ----------

df3.display()

# COMMAND ----------

from pyspark.sql.functions import when

# Assuming df3 is your DataFrame
df4 = df3.withColumn(
    "salary_rating",
    when(df3["salary"] > 3000, "high")
    .when(df3["salary"] <= 2000, "low")
    .otherwise("medium")
)

display(df4)

# COMMAND ----------

from pyspark.sql.functions import row_number,dense_rank
from pyspark.sql import Window
w = Window.partitionBy("department").orderBy(df4["salary"].desc())
display(df4.withColumn("salary_ranking", dense_rank().over(w)))

# COMMAND ----------


