# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC * Read from a csv folder 
# MAGIC * camel case to snake case

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

lap_time_df = spark.read.schema(lap_time_schema).csv('/mnt/formulaone32/raw/lap_times/lap_times_split_*.csv')

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

lap_times_final_df = lap_time_df.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingested_date', current_date())

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet('/mnt/formulaone32/processed/lap_times')
