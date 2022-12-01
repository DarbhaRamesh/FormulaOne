# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC * Process multiline json
# MAGIC * camel case to snake case
# MAGIC * add ingested date

# COMMAND ----------

# MAGIC %run "../configs/config"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType([
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('stop', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('time', StringType()),
    StructField('duration', StringType()),
    StructField('milliseconds', IntegerType())
])

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema).option('multiline', True).json(f'{raw_path}/pit_stops.json')

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

pitstops_final_df = add_metadata(pitstops_renamed_df)

# COMMAND ----------

pitstops_final_df.write.mode('overwrite').parquet('{processed_path}/pit_stops')
