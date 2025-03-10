# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating Flag parameter

# COMMAND ----------

dbutils.widgets.text('Incremental_Flag','0')

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get('Incremental_Flag')
print(Incremental_Flag), print(type(Incremental_Flag))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Creating Dimension Branch

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dealer Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching Relative Columns

# COMMAND ----------

df_src = spark.sql('''
                   select Distinct Dealer_ID, DealerName from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`
                   ''')
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_dealer sink intial and increamental load (Just Bring Schema if table does not exists)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    # Incremental Load
    df_sink= spark.sql('''
                    select dim_dealer_key, Dealer_ID, DealerName from cars_catalog.gold.dim_dealer
                        ''')
    

else:   
    # Intial Load
    df_sink= spark.sql('''
                    select 1 as dim_dealer_key, Dealer_ID, DealerName from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`
                        where 1=0
                        ''')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Dealer_ID == df_sink.Dealer_ID, 'left').select(df_src['Dealer_ID'], df_src['DealerName'], df_sink['dim_Dealer_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

 df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull()) 
 df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

 df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(df_src['Dealer_ID'], df_src['DealerName'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching Match surrogate Key from existing table

# COMMAND ----------

if (Incremental_Flag =='0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ***Create Surrogate key column and ADD the max Surrogate Key***

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value+ monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD(Slowly Changing Dimension) Type - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
 #Incremental Run
 delta_tbl = DeltaTable.forPath(spark, "abfss://gold@dlcarsalesproject.dfs.core.windows.net/dim_dealer")
 delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_dealer_key = src.dim_dealer_key')\
                        .whenMatchedUpdateAll()\
                          .whenNotMatchedInsertAll()\
                            .execute()

else: 
  #Intial Run
  df_final.write.format('delta')\
      .mode('overwrite')\
        .option("path", "abfss://gold@dlcarsalesproject.dfs.core.windows.net/dim_dealer")\
          .saveAsTable("cars_catalog.gold.dim_dealer")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cars_catalog.gold.dim_dealer

# COMMAND ----------

