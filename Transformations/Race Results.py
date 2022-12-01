# Databricks notebook source
# MAGIC %md 
# MAGIC * https://www.bbc.com/sport/formula1/2022/abu-dhabi-grand-prix/results/race

# COMMAND ----------

# MAGIC %run "./configs/config"

# COMMAND ----------

results_df = spark.read.parquet(f"{source_path}/results")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_selected_df = results_df.select(col("race_id"), 
                               col("driver_id"), 
                               col("constructor_id"), 
                               col("fastest_lap"), 
                               col("grid"), 
                               col("laps"), 
                               col("time").alias("race_time"),
                               col("points"))

# COMMAND ----------

races_df = spark.read.parquet(f"{source_path}/races")

# COMMAND ----------

races_selected_df = races_df.select(col("race_id"),
                                    col("year").alias("race_year"),
                                    col("name").alias("race_name"),
                                    col("race_timestamp"), 
                                    col("circuit_id"))

# COMMAND ----------

interim1 = results_selected_df.join(races_selected_df, results_selected_df.race_id == races_selected_df.race_id)

# COMMAND ----------

drivers_df = spark.read.parquet(f'{source_path}/drivers')

# COMMAND ----------

drivers_seleceted_df = drivers_df.select(col("driver_id"), 
                                         col("name").alias("driver_name"), 
                                         col("nationality").alias("driver_nationality"),
                                         col("number").alias("driver_number"))

# COMMAND ----------

interim2 = interim1.join(drivers_seleceted_df, interim1.driver_id == drivers_seleceted_df.driver_id)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{source_path}/constructors")

# COMMAND ----------

constructors_selected_df = constructors_df.select(col("constructor_id"),
                                                  col("name").alias("team"))

# COMMAND ----------

interim3 = interim2.join(constructors_selected_df, interim2.constructor_id == constructors_selected_df.constructor_id)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{source_path}/circuits")

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuit_id"), col("location").alias("circuit_location"))

# COMMAND ----------

interim4 = interim3.join(circuits_selected_df, interim3.circuit_id == circuits_selected_df.circuit_id)

# COMMAND ----------

interim5 = interim4.drop("race_id", "driver_id", "circuit_id" , "constructor_id")

# COMMAND ----------

interim5.write.mode("overwrite").partitionBy("race_year").parquet(f"{destination_path}/race_results")
