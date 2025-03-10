# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
            .option('inferSchema',True)\
            .load('abfss://bronze@dlcarsalesproject.dfs.core.windows.net/rawdata/')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Breaking Model_id column

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('model_category', split(col('Model_ID'),'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.withColumn('Units_Sold', col('Units_Sold').cast(StringType())).display()

# COMMAND ----------

df = df.withColumn('Rev_Per_unit', col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Analysis

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy('BranchName','Year').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold',ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing to Silver Container

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
            .option('path','abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales')\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Quering Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`abfss://silver@dlcarsalesproject.dfs.core.windows.net/carsales`

# COMMAND ----------

