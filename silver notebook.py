# Databricks notebook source
# MAGIC %md
# MAGIC # data reading

# COMMAND ----------

df=spark.read.format("parquet")\
        .option("inferschema",True)\
        .load("abfss://bronze@sunitadatabrickspro.dfs.core.windows.net/rawdata/")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # data transformation

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=df.withColumn("Model_Category",split(col("Model_ID"),"-")[0])
df.display()


# COMMAND ----------

from pyspark.sql.functions import col, cast
df.withColumn("Units_sold", col("Units_sold").cast("string")).printSchema()


# COMMAND ----------

df=df.withColumn("Unit_price",col("Revenue")/col("Units_sold"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # data aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC ### AD-HOC

# COMMAND ----------

df.display()

# COMMAND ----------



display(df.groupBy("Year", "BranchName").agg(sum("Units_sold").alias("Total_units_sold")).sort("Year","Total_units_sold",ascending=[1,0]))



# COMMAND ----------

# MAGIC %md
# MAGIC # data writing

# COMMAND ----------

df.write.format("parquet").mode("overwrite").option("path","abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC # quering silver data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
# MAGIC

# COMMAND ----------

