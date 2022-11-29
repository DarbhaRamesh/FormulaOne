# Databricks notebook source
# MAGIC %md
# MAGIC ## Requirements
# MAGIC * read json and write in parquet format
# MAGIC * convert camel case to snake case
# MAGIC * drop url and add ingested date
# MAGIC * Nested Json - name.forename and name.surname ->name

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

drivers_schema = StructType([
    StructField('code', StringType()),
    StructField('dob', DateType()),
    StructField('driverId', IntegerType()),
    StructField('driverRef', StringType()),
    StructField('name', StructType([StructField('forename', StringType()), StructField('surname', StringType())])),
    StructField('nationality', StringType()),
    StructField('number', IntegerType()),
    StructField('url', StringType())
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json('/mnt/formulaone32/raw/drivers.json')

# COMMAND ----------

drivers_dropped_df = drivers_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_date

# COMMAND ----------

drivers_final_df = drivers_dropped_df.withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
.withColumn('ingested_date', current_date())\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('driverRef', 'driver_ref')

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet('/mnt/formulaone32/processed/drivers')
