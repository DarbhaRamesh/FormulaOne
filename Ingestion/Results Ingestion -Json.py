# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC * Json to Parquet
# MAGIC * camel case to snake case
# MAGIC * drop statusId and add ingested date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType([
    StructField('constructorId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('fastestLap', IntegerType()),
    StructField('fastestLapSpeed', StringType()),
    StructField('fastestLapTime', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('milliseconds', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', FloatType()),
    StructField('position', IntegerType()),
    StructField('positionOrder', IntegerType()),
    StructField('positionText', StringType()),
    StructField('raceId', IntegerType()),
    StructField('rank', IntegerType()),
    StructField('resultId', IntegerType()),
    StructField('statusId', IntegerType()),
    StructField('time', StringType())
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json('/mnt/formulaone32/raw/results.json')

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('fastestLap', 'fastest_lap')\
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
.withColumnRenamed('fastestLapTime','fastest_lap_time')\
.withColumnRenamed('positionOrder','position_order')\
.withColumnRenamed('positionText', 'position_text')\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('resultId','result_id')

# COMMAND ----------

results_final_df =  results_renamed_df.drop('statusId')\
.withColumn('ingested_date', current_date())

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formulaone32/processed/results')
