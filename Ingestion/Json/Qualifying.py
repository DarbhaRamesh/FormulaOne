# Databricks notebook source
# MAGIC %run "../configs/config"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType([
    StructField('qualifyId', IntegerType()),
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('constructorId', IntegerType()),
    StructField('number', IntegerType()),
    StructField('position', IntegerType()),
    StructField('q1', StringType()),
    StructField('q2', StringType()),
    StructField('q3', StringType()),
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiline', True).json(f'{raw_path}/qualifying/qualifying_split*.json')

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

qualifying_final_df =  add_metadata(qualifying_renamed_df)

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet('{processed_path}/qualifying')
