# Databricks notebook source
raw_path = 'dbfs:/mnt/formulaone32/raw'
processed_path = 'dbfs:/mnt/formulaone32/processed'

# COMMAND ----------

from pyspark.sql.functions import current_date, lit

# COMMAND ----------

dbutils.widgets.text("Source of Data", "ergast.com")

# COMMAND ----------

def add_metadata(dataframe_to_add):
    new_dataframe = dataframe_to_add.withColumn('ingested_date', current_date()).withColumn('source', lit(dbutils.widgets.get('Source of Data')))
    return new_dataframe
