# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC **Project:** Uber/Lyft Databricks ELT Pipeline  
# MAGIC **Layer:** Bronze  
# MAGIC **Sources:** `cab_rides.csv` and `weather.csv`
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC - Ingest raw CSV files incrementally with Auto Loader.
# MAGIC - Apply explicit source schemas.
# MAGIC - Preserve source values without business transformations.
# MAGIC - Add ingestion timestamps and source-file metadata.
# MAGIC - Capture unexpected fields or type mismatches.
# MAGIC - Write governed Delta tables in Unity Catalog.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

# -------------------------------------------------------------------
# Source-volume paths
# Auto Loader monitors these directories for CSV files.
# -------------------------------------------------------------------

cab_source_path = (
    "/Volumes/rideshare_elt/bronze/cab_rides_files"
)

weather_source_path = (
    "/Volumes/rideshare_elt/bronze/weather_files"
)

# -------------------------------------------------------------------
# Auto Loader checkpoint paths
# Checkpoints remember which files have already been loaded
# -------------------------------------------------------------------

cab_checkpoint_path = (
    "/Volumes/rideshare_elt/bronze/pipeline_state/"
    "cab_rides_checkpoint"
)

weather_checkpoint_path = (
    "/Volumes/rideshare_elt/bronze/pipeline_state/"
    "weather_checkpoint"
)

# -------------------------------------------------------------------
# Unity Catalog target variables
# -------------------------------------------------------------------

cab_bronze_table = "rideshare_elt.bronze.cab_rides_raw"
weather_bronze_table = "rideshare_elt.bronze.weather_raw"

# -------------------------------------------------------------------
# Explicit Schema for cab_rides.csv
# -------------------------------------------------------------------

cab_schema = StructType([
    StructField("distance", DoubleType(), True),
    StructField("cab_type", StringType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("destination", StringType(), True),
    StructField("source", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True)
])

# -------------------------------------------------------------------
# Explicit Schema for weather.csv
# -------------------------------------------------------------------

weather_schema = StructType([
    StructField("temp", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("clouds", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind", DoubleType(), True)
])

print("Configuration and schemas created successfully.")

print("\nCab-rides target variables:")
print(cab_bronze_table)

print("\nWeather target variables:")
print(weather_bronze_table)

print("\nCab-rides schema:")
print(cab_schema.simpleString())

print("\nWeather schema:")
print(weather_schema.simpleString())

# COMMAND ----------

from pyspark.sql import functions as F

cab_bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(cab_schema)
        .load(cab_source_path)
        .select(
            "*",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
        )
)

print("Cab-rides Auto Loader DataFrame created.")
print(f"Is streaming: {cab_bronze_df.isStreaming}")

print("\nCab-rides Bronze schema:")
cab_bronze_df.printSchema()

# COMMAND ----------

cab_ingestion_query = (
    cab_bronze_df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", cab_checkpoint_path)
                .trigger(availableNow=True)
                .toTable(cab_bronze_table)
)

print("Cab-rides ingestion started.")

cab_ingestion_query.awaitTermination()

print("Cab-rides ingestion completed successfully.")
print(f"Bronze Table: {cab_bronze_table}")
print(f"Checkpoint: {cab_checkpoint_path}")

# COMMAND ----------

cab_bronze_table_df = spark.table(cab_bronze_table)

cab_bronze_summary_df = cab_bronze_table_df.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("id").alias("unique_ride_ids"),
    F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price_rows"),
    F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_rows"),
    F.countDistinct("_source_file_name").alias("source_file_count"),
    F.min("_ingested_at").alias("first_ingested_at"),
    F.max("_ingested_at").alias("last_ingested_at")
)

print("CAB-RIDES BRONZE SUMMARY")
display(cab_bronze_summary_df)

print("CAB-RIDES BRONZE SAMPLE")
display(
    cab_bronze_table_df.select(
        "id",
        "cab_type",
        "name",
        "distance",
        "price",
        "source",
        "destination",
        "_source_file_name",
        "_ingested_at",
        "_rescued_data"
    ).limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

weather_bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(weather_schema)
        .load(weather_source_path)
        .select(
            "*",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
        )
)

print("Weather Auto Loader DataFrame created.")
print(f"Is streaming: {weather_bronze_df.isStreaming}")

print("\nWeather Bronze Schema:")
weather_bronze_df.printSchema()

# COMMAND ----------

weather_ingestion_query = (
    weather_bronze_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", weather_checkpoint_path)
                    .trigger(availableNow=True)
                    .toTable(weather_bronze_table)
)

print("Weather ingestion started.")

weather_ingestion_query.awaitTermination()

print("\nWeather ingestion completed successfully.")
print(f"Bronze Table: {weather_bronze_table}")
print(f"Checkpoint: {weather_checkpoint_path}")

# COMMAND ----------

weather_bronze_table_df = spark.table(weather_bronze_table)

weather_bronze_summary_df = weather_bronze_table_df.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("location").alias("unique_locations"),
    F.countDistinct("time_stamp").alias("unique_weather_timestamps"),
    F.sum(F.when(F.col("rain").isNull(), 1).otherwise(0)).alias("null_rain_rows"),
    F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_rows"),
    F.countDistinct("_source_file_name").alias("source_file_count"),
    F.min("_ingested_at").alias("first_ingested_at"),
    F.max("_ingested_at").alias("last_ingested_at")
)

print("Weather Bronze Validation Summary:")
display(weather_bronze_summary_df)

print("Sample Weather Bronze records:")
display(
    weather_bronze_table_df.select(
        "temp",
        "location",
        "clouds",
        "pressure",
        "rain",
        "time_stamp",
        "humidity",
        "wind",
        "_source_file_name",
        "_ingested_at",
        "_rescued_data"
    ).limit(10)
)