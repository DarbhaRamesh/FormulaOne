# Databricks notebook source
# MAGIC %md
# MAGIC ## Requiements
# MAGIC 
# MAGIC * read Json and write to parquet
# MAGIC * convert camel case to snake case
# MAGIC * drop url and add ingestion date

# COMMAND ----------

# MAGIC %run "../configs/config"

# COMMAND ----------

constructors_schema = '''
    constructorId SHORT,
    constructorRef STRING,
    name STRING, 
    nationality STRING, 
    url STRING
'''

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f'{raw_path}/constructors.json')

# COMMAND ----------

constructors_drop_df= constructors_df.drop('url')

# COMMAND ----------

constructors_rename_df = constructors_drop_df.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

constructors_final_df = add_metadata(constructors_rename_df)

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet('{processed_path}/constructors')

# COMMAND ----------

dbutils.notebook.exit("Success")
