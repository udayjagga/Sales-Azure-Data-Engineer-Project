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
# MAGIC #Creating Dimension Model

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Model Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching Relative Columns

# COMMAND ----------

df_src = spark.sql('''
                   select Distinct Model_ID, Model_category from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`
                   ''')
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_Model sink intial and increamental load

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    # Incremental Load
    df_sink= spark.sql('''
                    select dim_model_key, Model_ID, model_category from cars_catalog.gold.dim_model
                        ''')
    

else:   
    # Intial Load
    df_sink= spark.sql('''
                    select 1 as dim_model_key, Model_ID, model_category from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`
                        where 1=0
                        ''')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Model_ID == df_sink.Model_ID, 'left').select(df_src['Model_ID'], df_src['Model_category'], df_sink['dim_model_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

 df_filter_old = df_filter.filter(col('dim_model_key').isNotNull()) 
 df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

 df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_src['Model_ID'], df_src['Model_category'])

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
    max_value_df = spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ***Create Surrogate key column and ADD the max Surrogate Key***

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value+ monotonically_increasing_id())

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

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
 #Incremental Run
 delta_tbl = DeltaTable.forPath(spark, "abfss://gold@dlcarsalesproject.dfs.core.windows.net/dim_model")
 delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key = src.dim_model_key')\
                        .whenMatchedUpdateAll()\
                          .whenNotMatchedInsertAll()\
                            .execute()

else: 
  #Intial Run
  df_final.write.format('delta')\
      .mode('overwrite')\
        .option("path", "abfss://gold@dlcarsalesproject.dfs.core.windows.net/dim_model")\
          .saveAsTable("cars_catalog.gold.dim_model")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------

