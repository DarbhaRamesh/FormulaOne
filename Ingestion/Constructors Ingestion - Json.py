# Databricks notebook source
# MAGIC %md
# MAGIC ## Requiements
# MAGIC 
# MAGIC * read Json and write to parquet
# MAGIC * convert camel case to snake case
# MAGIC * drop url and add ingestion date

# COMMAND ----------

constructors_schema = '''
    constructorId SHORT,
    constructorRef STRING,
    name STRING, 
    nationality STRING, 
    url STRING
'''

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json('/mnt/formulaone32/raw/constructors.json')

# COMMAND ----------

constructors_drop_df= constructors_df.drop('url')

# COMMAND ----------

constructors_rename_df = constructors_drop_df.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

constructors_final_df = constructors_rename_df.withColumn('ingested_date', current_date())

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet('/mnt/formulaone32/processed/constructors')
