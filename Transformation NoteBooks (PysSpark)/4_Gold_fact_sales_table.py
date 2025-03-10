# Databricks notebook source
# MAGIC %md
# MAGIC # Creating Fact Table

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Silver Data

# COMMAND ----------


df_silver = spark.sql("select * from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading all the dimensions tables

# COMMAND ----------

df_model = spark.sql("SELECT * FROM cars_catalog.gold.dim_model")

df_branch = spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")

df_dealer = spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")

df_date = spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bring Dim keys to the fact tables from dim tables

# COMMAND ----------

df_fact = df_silver.join(df_model, df_silver.Model_ID == df_model.Model_ID, how='left')\
                    .join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, how='left')\
                      .join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, how='left')\
                        .join(df_date, df_silver.Date_ID == df_date.Date_ID, how='left')\
                  .select(df_silver['Revenue'], df_silver['Units_Sold'], df_silver['Rev_Per_Unit'], df_model['dim_model_key'],
                          df_branch['dim_branch_key'], df_dealer['dim_dealer_key'], df_date['dim_date_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.factsales'):
  deltatable = DeltaTable.forName(spark, 'cars_catalog.gold.factsales')
  
  deltatable.alias('trg').merge(df_fact.alias('src'), 'trg.dim_model_key = src.dim_model_key and trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_date_key = src.dim_date_key')\
                          .whenMatchedUpdateAll()\
                          .whenNotMatchedInsertAll()\
                          .execute()


else:
  df_fact.write.format('delta')\
                  .mode('overwrite')\
                    .option("path", "abfss://gold@dlcarsalesproject.dfs.core.windows.net/factsales")\
                  .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales

# COMMAND ----------

