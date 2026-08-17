# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # create flag parameter

# COMMAND ----------

dbutils.widgets.text("incremental_flag","0")

# COMMAND ----------

incremental_flag=dbutils.widgets.get("incremental_flag")
print(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC # creating dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC ### fetch related column

# COMMAND ----------

df_src=spark.sql('''
select distinct(Model_ID) as Model_ID, Model_Category from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
''')
df_src.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model sink-initial and incremental 

# COMMAND ----------

# DBTITLE 1,Cell 10
if spark.catalog.tableExists("cars_catalog.goldd.dim_model"):
    df_sink=spark.sql('''
    select dim_model_key,Model_ID,Model_Category
    from cars_catalog.goldd.dim_model
    ''')
else:
    df_sink=spark.sql('''
    select 1 as dim_model_key,Model_ID,Model_Category
    from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
    where 1=0
    ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filterning new reocrds and old records

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src["Model_ID"]==df_sink["Model_ID"],"left").select(df_src["Model_ID"],df_src["Model_Category"],df_sink["dim_model_key"])


# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

# MAGIC %md
# MAGIC #### inital run

# COMMAND ----------

from pyspark.sql.functions import col
df_filter_old=df_filter.filter(col("dim_model_key").isNotNull())


# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col 
df_filter_new=df_filter.filter(col("dim_model_key").isNull()).select(df_src["Model_ID"],df_src["Model_Category"])

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC #### fetch max surrogate key from existing table

# COMMAND ----------

if (incremental_flag == "0"):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_model_key) from cars_catalog.goldd.dim_model")
    max_value=max_value_df.collect()[0][0]    

# COMMAND ----------

# MAGIC %md
# MAGIC #### create surrogate key column and add max surrogate key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_model_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### create final df ..df_filter_old+df_filter_new

# COMMAND ----------

df_final=df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1- Upsert
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# incremental run
if spark.catalog.tableExists("cars_catalog.goldd.dim_model"):
    deltaTable = DeltaTable.forPath(spark,"abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_model")

    deltaTable.alias("trg").merge(
        df_final.alias("src"),
        "trg.dim_model_key = src.dim_model_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final.write.format('delta').mode('overwrite').option("path","abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_model").saveAsTable("cars_catalog.goldd.dim_model")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.goldd.dim_model

# COMMAND ----------

# MAGIC %md
# MAGIC # dim_branch
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### fetch related columns

# COMMAND ----------

df_src2=spark.sql('''
     select distinct(Branch_ID)as Branch_ID,BranchName from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
''')
df_src2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DIM_BRANCH SINK- initial and incremental 

# COMMAND ----------

# DBTITLE 1,Cell 38
if spark.catalog.tableExists("cars_catalog.goldd.dim_dealer"):
    df_sink2=spark.sql('''
     select dim_branch_key,Branch_ID,BranchName from  cars_catalog.goldd.dim_branch
''')
    
else:
    df_sink2=spark.sql('''
     select  1 as dim_branch_key,Branch_ID,BranchName from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
     where 1=0
''')
    
df_sink2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filtering old records and new records

# COMMAND ----------

df_filter2=df_src2.join(df_sink2,df_src2["Branch_ID"]==df_sink2["Branch_ID"],"left").select(df_src2["Branch_ID"],df_src2["BranchName"],df_sink2["dim_branch_key"])

df_filter2.display()



# COMMAND ----------

from pyspark.sql.functions import col 
df_filter_old2=df_filter2.filter(col("dim_branch_key").isNotNull())

df_filter_old2.display()

# COMMAND ----------

df_filter_new2=df_filter2.filter(col("dim_branch_key").isNull()).select(df_src2["Branch_ID"],df_src2["BranchName"])
df_filter_new2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC ### fetch the max surrogate key from existing table

# COMMAND ----------

if (incremental_flag=='0'):
    max_value=1
else:
    max_value_df1=spark.sql("select max(dim_branch_key) from cars_catalog.goldd.dim_branch")
    max_value=max_value_df1.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surroagte key column and add the max surrogate key

# COMMAND ----------

df_filter_new2=df_filter_new2.withColumn("dim_branch_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create final df - df_filter_old2+df_filter_new2
# MAGIC

# COMMAND ----------

df_final2=df_filter_new2.union(df_filter_old2)
df_final2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE 1 UPSERT 

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Incremental run
# incremental run
if spark.catalog.tableExists("cars_catalog.goldd.dim_branch"):
    delta_table2=DeltaTable.forPath(spark,"abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_branch")

    delta_table2.alias("trg2").merge(df_final2.alias("src2"),"trg2.dim_branch_key=src2.dim_branch_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll().execute()

else:
    df_final2.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_branch")\
        .saveAsTable("cars_catalog.goldd.dim_branch")
      

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.goldd.dim_branch

# COMMAND ----------

# MAGIC %md
# MAGIC # dim dealer

# COMMAND ----------

df_src3=spark.sql('''
     select distinct(Dealer_ID)as Dealer_ID,DealerName from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
''')
df_src3.display()

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.goldd.dim_dealer"):
    df_sink3=spark.sql('''
     select dim_dealer_key,Dealer_ID,DealerName from from cars_catalog.goldd.dim_dealer
''')
    
else:
    df_sink3=spark.sql('''
     select  1 as dim_dealer_key,Dealer_ID,DealerName from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
     where 1=0
''')
    
df_sink3.display()


# COMMAND ----------

df_filter3=df_src3.join(df_sink3,df_src3["Dealer_ID"]==df_sink3["Dealer_ID"],"left").select(df_src3["Dealer_ID"],df_src3["DealerName"],df_sink3["dim_dealer_key"])


# COMMAND ----------

df_filter3.display()

# COMMAND ----------

from pyspark.sql.functions import col
df_filter_old3=df_filter3.filter(col("dim_dealer_key").isNotNull())


# COMMAND ----------

df_filter_old3.display()

# COMMAND ----------

from pyspark.sql.functions import col 
df_filter_new3=df_filter3.filter(col("dim_dealer_key").isNull()).select(df_src3["Dealer_ID"],df_src3["DealerName"])

df_filter_new3.display()

# COMMAND ----------

if (incremental_flag == "0"):
    max_value=1
else:
    max_value_df3=spark.sql("select max(dim_dealer_key) from cars_catalog.goldd.dim_dealer")
    max_value=max_value_df3.collect()[0][0] 

# COMMAND ----------

df_filter_new3=df_filter_new3.withColumn("dim_dealer_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new3.display()

# COMMAND ----------

df_final3=df_filter_new3.union(df_filter_old3)

# COMMAND ----------

df_final3.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cell 69
# incremental run
if spark.catalog.tableExists("cars_catalog.goldd.dim_dealer"):
    deltaTable3 = DeltaTable.forPath(spark,"abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_dealer")

    deltaTable3.alias("trg3").merge(
        df_final3.alias("src3"),
        "trg3.dim_dealer_key = src3.dim_dealer_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final3.write.format('delta').mode('overwrite').option("path","abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_dealer").saveAsTable("cars_catalog.goldd.dim_dealer")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.goldd.dim_dealer

# COMMAND ----------

# MAGIC %md
# MAGIC ### create dim_date

# COMMAND ----------

df_src4=spark.sql('''
     select distinct(Date_ID)as Date_ID from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
''')
df_src4.display()

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.goldd.dim_date"):
    df_sink4=spark.sql('''
     select dim_date_key,Date_ID from from cars_catalog.goldd.dim_date
''')
    
else:
    df_sink4=spark.sql('''
     select  1 as dim_date_key,Date_ID from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`
     where 1=0
''')
    
df_sink4.display()


# COMMAND ----------

df_filter4=df_src4.join(df_sink4,df_src4["Date_ID"]==df_sink4["Date_ID"],"left").select(df_src4["Date_ID"],df_sink4["dim_date_key"])

# COMMAND ----------

df_filter4.display()

# COMMAND ----------

from pyspark.sql.functions import col
df_filter_old4=df_filter4.filter(col("dim_date_key").isNotNull())


# COMMAND ----------

df_filter_old4.display()

# COMMAND ----------

from pyspark.sql.functions import col 
df_filter_new4=df_filter4.filter(col("dim_date_key").isNull()).select(df_src4["Date_ID"])

df_filter_new4.display()

# COMMAND ----------

if (incremental_flag == "0"):
    max_value=1
else:
    max_value_df4=spark.sql("select max(dim_date_key) from cars_catalog.goldd.dim_date")
    max_value=max_value_df4.collect()[0][0] 

# COMMAND ----------

df_filter_new4=df_filter_new4.withColumn("dim_date_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new4.display()

# COMMAND ----------

df_final4=df_filter_new4.union(df_filter_old4)

# COMMAND ----------

df_final4.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cell 85
# incremental run
if spark.catalog.tableExists("cars_catalog.goldd.dim_date"):
    deltaTable4 = DeltaTable.forPath(spark,"abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_date")

    deltaTable4.alias("trg4").merge(
        df_final4.dropDuplicates(["dim_date_key"]).alias("src4"),
        "trg4.dim_date_key = src4.dim_date_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final4.write.format('delta').mode('overwrite').option("path","abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/dim_date").saveAsTable("cars_catalog.goldd.dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.goldd.dim_date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # gold fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## create fact table

# COMMAND ----------

# MAGIC %md
# MAGIC ### reading silver data

# COMMAND ----------


df_silverr= spark.sql("select * from parquet.`abfss://silverr@sunitadatabrickspro.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silverr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### reading all dimensions

# COMMAND ----------

df_dealer=spark.sql("select * from cars_catalog.goldd.dim_dealer")
df_branch=spark.sql("select * from cars_catalog.goldd.dim_branch")
df_model=spark.sql("select * from cars_catalog.goldd.dim_model")
df_date=spark.sql("select * from cars_catalog.goldd.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### bringing keys to fact tale

# COMMAND ----------

df_fact=df_silverr.join(df_dealer,df_silverr['Dealer_ID']==df_dealer['Dealer_ID'],how='left')\
    .join(df_branch,df_silverr['Branch_ID']==df_branch['Branch_ID'],how='left')\
    .join(df_model,df_silverr['Model_ID']==df_model['Model_ID'],how='left')\
        .join(df_date,df_silverr['Date_ID']==df_date['Date_ID'],how='left')\
            .select(df_silverr["Revenue"],df_silverr["Units_sold"],df_silverr["Unit_price"],df_dealer["dim_dealer_key"],df_branch["dim_branch_key"],df_model["dim_model_key"],df_date["dim_date_key"])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing fact table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("factsales"):
    deltafact = DeltaTable.forName(spark,"cars_catalog.goldd.factsales")

    deltafact.alias("trgfact").merge(
        df_fact.alias("srcfact"),
        "trgfact.dim_dealer_key = srcfact.dim_dealer_key and trgfact.dim_branch_key=srcfact.dim_branch_key and trgfact.dim_model_key=srcfact.dim_model_key and trgfact.dim_date_key=srcfact.dim_date_key"
    )\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact.write.format('delta').mode('overwrite').option("path","abfss://goldd@sunitadatabrickspro.dfs.core.windows.net/factsales").saveAsTable("cars_catalog.goldd.factsales")




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.goldd.factsales

# COMMAND ----------

