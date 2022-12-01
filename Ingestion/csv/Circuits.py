# Databricks notebook source
# MAGIC %md
# MAGIC ## DataSource API - [DataFrame Reader](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv) Method
# MAGIC 
# MAGIC * Circuits.csv

# COMMAND ----------

# MAGIC %run "../configs/config"

# COMMAND ----------

from  pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema=StructType([
    StructField('circuitId', IntegerType()),
    StructField('circuitRef', StringType()),
    StructField('name', StringType()),
    StructField('location', StringType()),
    StructField('country', StringType()),
    StructField('lat', DoubleType()),
    StructField('lng', DoubleType()),
    StructField('alt', IntegerType()),
    StructField('url', StringType())
])

# COMMAND ----------

circuits_df = spark.read.option('header',True).schema(circuits_schema).csv(f'{raw_path}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Require Columns

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col('circuitId').alias('circuit_id'), col('circuitRef'), col('name'), col('location'),col('country'),col('lat'),col('lng'),col('alt')) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming Columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitRef','circuit_ref')\
.withColumnRenamed('lat','latitude')\
.withColumnRenamed('lng','longitude')\
.withColumnRenamed('alt','altitude')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Ingestion Date

# COMMAND ----------

circuits_final_df = add_metadata(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write to parquet

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet('{processed_path}/circuits')
