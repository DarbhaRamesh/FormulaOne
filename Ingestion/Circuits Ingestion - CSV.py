# Databricks notebook source
# MAGIC %md
# MAGIC ## DataSource API - [DataFrame Reader](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv) Method
# MAGIC 
# MAGIC * Circuits.csv

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

circuits_df = spark.read.option('header',True).schema(circuits_schema).csv('dbfs:/mnt/formulaone32/raw/circuits.csv')

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

from pyspark.sql.functions import current_date,lit

# COMMAND ----------

circuits_final_df = circuits_selected_df.withColumn('ingested_date', current_date())
# .withColumn('is_course', lit(True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## write to parquet

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet('/mnt/formulaone32/processed/circuits')
