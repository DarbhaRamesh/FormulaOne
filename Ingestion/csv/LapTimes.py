# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC * Read from a csv folder 
# MAGIC * camel case to snake case

# COMMAND ----------

# MAGIC %run "../configs/config"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_time_schema = StructType([
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('position', IntegerType()),
    StructField('time', StringType()),
    StructField('millisecond', IntegerType())
])

# COMMAND ----------

lap_time_df = spark.read.schema(lap_time_schema).csv(f'{raw_path}/lap_times/lap_times_split_*.csv')

# COMMAND ----------

lap_times_renamed_df = lap_time_df.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

lap_times_final_df = add_metadata(lap_times_renamed_df)

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet(f'{processed_path}/lap_times')

# COMMAND ----------

dbutils.notebook.exit("Success")
