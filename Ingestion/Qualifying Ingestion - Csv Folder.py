# Databricks notebook source
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

qualifying_df = spark.read.schema(qualifying_schema).option('multiline', True).json('/mnt/formulaone32/raw/qualifying')

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('constructorId', 'constructor_id')\
.withColumn('ingested_date', current_date())

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet('/mnt/formulaone32/processed/qualifying')
