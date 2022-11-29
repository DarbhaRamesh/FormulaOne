# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC * Process multiline json
# MAGIC * camel case to snake case
# MAGIC * add ingested date

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

pitstops_df = spark.read.schema(pitstops_schema).option('multiline', True).json('/mnt/formulaone32/raw/pit_stops.json')

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumn('ingested_date', current_date())

# COMMAND ----------

pitstops_final_df.write.mode('overwrite').parquet('/mnt/formulaone32/processed/pit_stops')
