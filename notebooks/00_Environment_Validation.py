# Databricks notebook source
cab_volume_path = "/Volumes/rideshare_elt/bronze/cab_rides_files"
weather_volume_path = "/Volumes/rideshare_elt/bronze/weather_files"

cab_files = dbutils.fs.ls(cab_volume_path)
weather_files = dbutils.fs.ls(weather_volume_path)

print("CAB RIDE FILES")
for file in cab_files:
    print(
        f"Name: {file.name} | "
        f"Size: {file.size:,} bytes | "
        f"Path: {file.path}"
    )

print("\nWEATHER FILES")
for file in weather_files:
    print(
        f"Name: {file.name} | "
        f"Size: {file.size:,} bytes | "
        f"Path: {file.path}"
    )

# COMMAND ----------

cab_file_path = "/Volumes/rideshare_elt/bronze/cab_rides_files/cab_rides.csv"

cab_preview_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cab_file_path)
)

print("CAB RIDES SCHEMA")
cab_preview_df.printSchema()

print("CAB RIDES COLUMNS")
print(cab_preview_df.columns)

display(cab_preview_df.limit(10))

# COMMAND ----------

weather_file_path = "/Volumes/rideshare_elt/bronze/weather_files/weather.csv"

weather_preview_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(weather_file_path)
)

print("WEATHER SCHEMA")
weather_preview_df.printSchema()

print("WEATHER COLUMNS")
print(weather_preview_df.columns)

display(weather_preview_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# Create one null-count expression for every cab-rides column.
cab_null_expressions = [
    F.sum(
        F.when(F.col(column_name).isNull(), 1).otherwise(0)
    ).alias(f"null_{column_name}")
    for column_name in cab_preview_df.columns
]

cab_quality_df = cab_preview_df.agg(
    # Total Rows
    F.count("*").alias("total_rows"),

    # Distinct Ride Ids
    F.countDistinct("id").alias("unique_ride_ids"),

    # Null Counts
    *cab_null_expressions,

    # Non-Positive Distance rows
    F.sum(F.when(F.col("distance") <= 0, 1).otherwise(0)).alias("non_positive_distance_rows"),

    # Negative Price rows
    F.sum(F.when(F.col("price") < 0, 1).otherwise(0)).alias("negative_price_rows"),

    # Surge below one rows
    F.sum(F.when(F.col("surge_multiplier") < 1, 1).otherwise(0)).alias("surge_below_one_rows")
)

# Create one null-count expression for every weather column
weather_null_expressions = [
    F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias(f"null_{column_name}")
    for column_name in weather_preview_df.columns
]

weather_quality_df = weather_preview_df.agg(
    # Total rows
    F.count("*").alias("total_rows"),

    # Distinct Locations
    F.countDistinct("location").alias("unique_locations"),

    # Distinct TimeStamps
    F.countDistinct("time_stamp").alias("unique_weather_timestamps"),

    # Null Counts
    *weather_null_expressions,

    # Invalid Humidity rows
    F.sum(F.when((F.col("humidity") < 0) | (F.col("humidity") > 1), 1).otherwise(0))
    .alias("invalid_humidity_rows"),

    # Invalid Pressure rows
    F.sum(F.when(F.col("pressure") <= 0, 1).otherwise(0)).alias("invalid_pressure_rows"),

    # Invalid Wind rows
    F.sum(F.when(F.col("wind") < 0, 1).otherwise(0)).alias("invalid_wind_rows")
)

print("CAB RIDES DATA-QUALITY SUMMARY")
display(cab_quality_df)

print("WEATHER DATA-QUALITY SUMMARY")
display(weather_quality_df)