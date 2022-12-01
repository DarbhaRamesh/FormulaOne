# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC 
# MAGIC * Read csv file and write in parquet format
# MAGIC * Drop url
# MAGIC * convert camel case column names to snake case
# MAGIC * combine data and year to a column race timestamp
# MAGIC * add ingestion date column

# COMMAND ----------

# MAGIC %run "../configs/config"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType([
    StructField('raceId', IntegerType()),
    StructField('year', IntegerType()),
    StructField('round', IntegerType()),
    StructField('circuitId', IntegerType()),
    StructField('name', StringType()),
    StructField('date', DateType()),
    StructField('time', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

races_df = spark.read.option('header', True).schema(races_schema).csv(f'{raw_path}/races.csv')

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat,lit

# COMMAND ----------

races_addcolumn_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_selected_df = races_addcolumn_df.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('race_timestamp'))

# COMMAND ----------

races_rename_df = races_selected_df.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('circuitId','circuit_id')

# COMMAND ----------

races_final_df = add_metadata(races_rename_df)

# COMMAND ----------

races_final_df.write.mode('overwrite').parquet(f'{processed_path}/races')

# COMMAND ----------

dbutils.notebook.exit("Success")
