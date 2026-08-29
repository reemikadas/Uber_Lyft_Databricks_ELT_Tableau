# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformations
# MAGIC
# MAGIC **Project:** Uber/Lyft Databricks ELT Pipeline  
# MAGIC **Source layer:** Bronze  
# MAGIC **Target layer:** Silver  
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook transforms raw Bronze data into clean, standardized, and analytics-ready Silver tables.
# MAGIC
# MAGIC ### Cab-rides transformations
# MAGIC
# MAGIC - Convert the Unix timestamp into a readable timestamp.
# MAGIC - Create useful date and time attributes.
# MAGIC - Standardize text columns.
# MAGIC - Separate valid-price rides from records with missing prices.
# MAGIC - Check for duplicate ride IDs.
# MAGIC - Add data-quality indicators.
# MAGIC
# MAGIC ### Weather transformations
# MAGIC
# MAGIC - Convert the Unix timestamp into a readable timestamp.
# MAGIC - Replace missing rain measurements with `0.0`.
# MAGIC - Preserve whether the original rain measurement was missing.
# MAGIC - Standardize location values.
# MAGIC - Create useful date and time attributes.
# MAGIC
# MAGIC ### Silver outputs
# MAGIC
# MAGIC - `rideshare_elt.silver.cab_rides_clean`
# MAGIC - `rideshare_elt.silver.cab_rides_quarantine`
# MAGIC - `rideshare_elt.silver.weather_clean`
# MAGIC
# MAGIC ## Quarantine strategy
# MAGIC
# MAGIC Cab records with missing prices cannot be used for price analysis. They will be stored in a separate quarantine table instead of being deleted.
# MAGIC
# MAGIC This preserves auditability while keeping the clean analytical dataset reliable.

# COMMAND ----------

# Configure the Silver notebook and load Bronze tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Bronze Source Tables
cab_bronze_table = "rideshare_elt.bronze.cab_rides_raw"
weather_bronze_table = "rideshare_elt.bronze.weather_raw"

# Silver Target tables
cab_silver_table = "rideshare_elt.silver.cab_rides_clean"
cab_quarantine_table = "rideshare_elt.silver.cab_rides_quarantine"
weather_silver_table = "rideshare_elt.silver.weather_clean"

# Confirm that both required Bronze tables exist
required_bronze_tables = [
    cab_bronze_table,
    weather_bronze_table
]

missing_tables = [
    table_name
    for table_name in required_bronze_tables
    if not spark.catalog.tableExists(table_name)
]

if missing_tables:
    raise ValueError(f"Required Bronze tables were not found: {missing_tables}")

# Load the Bronze Delta tables
cab_bronze_df = spark.table(cab_bronze_table)
weather_bronze_df = spark.table(weather_bronze_table)

# Count the source records
cab_bronze_count = cab_bronze_df.count()
weather_bronze_count = weather_bronze_df.count()

print("Silver notebook configuration completed.")
print(f"Cab-rides Bronze rows: {cab_bronze_count:,}")
print(f"Weather Bronze rows: {weather_bronze_count:,}")
print()
print("Silver Target Variables:")
print(f" Clean cab rides: {cab_silver_table}")
print(f" Quarantined cab rides: {cab_quarantine_table}")
print(f" Clean weather: {weather_silver_table}")

# COMMAND ----------

# Verify the timestamp units

# Use UTC while interpreting Unix timestamps
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Profile cab-rides timestamps
cab_timestamp_check_df = (
    cab_bronze_df.agg(
        F.min("time_stamp").alias("minimum_raw_timestamp"),
        F.max("time_stamp").alias("maximum_raw_timestamp")
    )
    .select(
        "*",
        F.length(F.col("minimum_raw_timestamp").cast("string")).alias("timestamp_digits"),
        F.to_timestamp(F.from_unixtime(F.col("minimum_raw_timestamp")/1000)).alias("minimum_utc_datetime"),
        F.to_timestamp(F.from_unixtime(F.col("maximum_raw_timestamp")/1000)).alias("maximum_utc_datetime")
    )
)

print("Cab-rides timestamp profile:")
display(cab_timestamp_check_df)

# COMMAND ----------

# Profile weather timestamp
weather_timestamp_check_df = (
    weather_bronze_df.agg(
        F.min("time_stamp").alias("minimum_raw_timestamp"),
        F.max("time_stamp").alias("maximum_raw_timestamp")
    )
    .select(
        "*",
        F.length(F.col("minimum_raw_timestamp").cast("string")).alias("timestamp_digits"),
        F.expr("timestamp_seconds(minimum_raw_timestamp)").alias("minimum_utc_datetime"),
        F.expr("timestamp_seconds(maximum_raw_timestamp)").alias("maximum_utc_datetime")
    )
)

print("Corrected weather timestamp profile:")
display(weather_timestamp_check_df)

# COMMAND ----------

# Transform and standardize the cab-rides data

cab_standardized_df = (
    cab_bronze_df
        # Remove accidental spaces while preserving original capitalization
        .withColumn("cab_type", F.trim(F.col("cab_type")))
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("source", F.trim(F.col("source")))
        .withColumn("destination", F.trim(F.col("destination")))

        # Convert the 13-digit millisecond timestamp to UTC
        .withColumn(
            "ride_datetime_utc",
            F.expr("timestamp_millis(time_stamp)")
            )
        
        # Convert UTC into Boston local time
        .withColumn(
            "ride_datetime_local",
            F.from_utc_timestamp(
                F.col("ride_datetime_utc"),
                "America/New_York"
            )
        )

        # Create fields that will be useful in Tableau
        .withColumn(
            "ride_date_local",
            F.to_date(F.col("ride_datetime_local"))
        )
        .withColumn(
            "ride_hour_local",
            F.hour(F.col("ride_datetime_local"))
        )
        .withColumn(
            "ride_day_name_local",
            F.date_format(F.col("ride_datetime_local"), "EEEE")
        )
        .withColumn(
            "is_weekend",
            F.dayofweek(F.col("ride_datetime_local")).isin([1,7])
        )

        # Add pricing analysis fields
        .withColumn(
            "surge_applied",
            F.col("surge_multiplier") > 1.0
        )
        .withColumn(
            "price_per_mile",
            F.when(
                F.col("price").isNotNull() & (F.col("distance") > 0),
                F.round(F.col("price") / F.col("distance"), 2)
            )
        )

        # Assign data-quality status to every record
        .withColumn(
            "quality_status",
            F.when(
                F.col("id").isNull() |
                (F.trim(F.col("id")) == ""),
                F.lit("QUARANTINE_MISSING_ID")
            )
            .when(
                F.col("price").isNull(),
                F.lit("QUARANTINE_MISSING_PRICE")
            )
            .when(
                F.col("price") < 0,
                F.lit("QUARANTINE_INVALID_PRICE")
            )
            .when(
                F.col("distance") <= 0,
                F.lit("QUARANTINE_INVALID_DISTANCE")
            )
            .otherwise(F.lit("VALID"))
        )

        # Record when the Silver transformation was performed
        .withColumn(
            "_silver_transformed_at",
            F.current_timestamp()
        )
)

print("Cab-rides standardized DataFrame created.")
print(f"Rows available for transformation: {cab_standardized_df.count():,}")

display(
    cab_standardized_df.select(
        "id",
        "cab_type",
        "name",
        "source",
        "destination",
        "price",
        "surge_multiplier",
        "ride_datetime_utc",
        "ride_datetime_local",
        "ride_date_local",
        "ride_hour_local",
        "ride_day_name_local",
        "is_weekend",
        "surge_applied",
        "price_per_mile",
        "quality_status"
    ).limit(10)
)

# COMMAND ----------

# Validate the standardized cab-rides DataFrame
cab_quality_summary_df = (
    cab_standardized_df
        .groupBy("quality_status")
        .agg(F.count("*").alias("row_count"))
        .orderBy("quality_status")
)

print("Cab-rides quality status distribution:")
display(cab_quality_summary_df)

cab_standardized_validation_df =(
    cab_standardized_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("id").alias("unique_ride_ids"),
            F.sum(F.when(F.col("ride_datetime_utc").isNull(), 1).otherwise(0)).alias("null_utc_datetimes"),
            F.sum(F.when(F.col("ride_datetime_local").isNull(), 1).otherwise(0)).alias("null_local_datetimes"),
            F.min("ride_datetime_local").alias("minimum_local_datetime"),
            F.max("ride_datetime_local").alias("maximum_local_datetime")
        )
)

print("Cab-rides standardized validation:")
display(cab_standardized_validation_df)

# Detect ride IDs that appear more than once
duplicate_ride_ids_df = (
    cab_standardized_df
        .groupBy("id")
        .agg(F.count("*").alias("record_count"))
        .filter(F.col("record_count") > 1)
)

duplicate_ride_id_count = duplicate_ride_ids_df.count()

print(f"Duplicate ride IDs: {duplicate_ride_id_count:,}")

# COMMAND ----------

# Write clean and quarantined cab rides to Silver

# Split standardized cab rides into clean and quarantine DataFrames
cab_clean_df = (
    cab_standardized_df
        .filter(F.col("quality_status") == "VALID")
)

cab_quarantine_df = (
    cab_standardized_df
        .filter(F.col("quality_status") != "VALID")
)

# Validate the split before writing
cab_clean_count = cab_clean_df.count()
cab_quarantine_count = cab_quarantine_df.count()
cab_split_total = cab_clean_count + cab_quarantine_count

print(f"Clean cab-rides rows: {cab_clean_count:,}")
print(f"Quarantine cab-rides rows: {cab_quarantine_count:,}")
print(f"Combined rows: {cab_split_total:,}")

# Stop execution if any source rows were lost during the split
if cab_split_total != cab_bronze_count:
    raise ValueError(
        "Cab-rides split validation failed. "
        f"Bronze rows: {cab_bronze_count:,}"
        f"Split rows: {cab_split_total:,}"
    )


# Write Valid records to the clean Silver Delta Table
(
    cab_clean_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cab_silver_table)
)

print(f"Clean Silver table written: {cab_silver_table}")

# Write Rejected records to the quarantine Silver Delta table
(
    cab_quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cab_quarantine_table)
)

print(f"Quarantine Silver table written: {cab_quarantine_table}")
print("Cab-rides Silver write completed successfully.")

# COMMAND ----------

# Validate the saved cab-rides Silver tables

# Read the persisted Silver Delta Tables
cab_clean_written_df = spark.table(cab_silver_table)
cab_quarantine_written_df = spark.table(cab_quarantine_table)

# Validate the clean Silver table
cab_clean_written_summary_df = (
    cab_clean_written_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("id").alias("unique_ride_ids"),
            F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price_rows"),
            F.sum(F.when(F.col("quality_status") != "VALID",1).otherwise(0)).alias("non_valid_rows"),
            F.sum(F.when(F.col("ride_datetime_local").isNull(),1).otherwise(0)).alias("null_local_datetimes")
        )
)

print("Persisted clean cab-rides table:")
display(cab_clean_written_summary_df)

# Validate the quarantine Silver Table
cab_quarantine_written_summary_df = (
    cab_quarantine_written_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("id").alias("unique_ride_ids"),
            F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price_rows"),
            F.sum(F.when(F.col("quality_status") == "VALID", 1).otherwise(0)).alias("unexpected_valid_rows")
        )
)

print("Persisted quarantine cab-rides table:")
display(cab_quarantine_written_summary_df)

# Show every quarantine reason
print("Quarantine Reasons:")
display(
    cab_quarantine_written_df
        .groupBy("quality_status")
        .agg(F.count("*").alias("row_count"))
        .orderBy("quality_status")
)

# Reconcile persisted Silver counts with Bronze
persisted_clean_count = cab_clean_written_df.count()
persisted_quarantine_count = cab_quarantine_written_df.count()
persisted_total = persisted_clean_count + persisted_quarantine_count

print(f"Persisted clean rows: {persisted_clean_count:,}")
print(f"Persisted quarantine rows: {persisted_quarantine_count:,}")
print(f"Persisted combined rows: {persisted_total:,}")
print(f"Original Bronze rows: {cab_bronze_count:,}")

if persisted_total != cab_bronze_count:
    raise ValueError("Persisted Silver row counts do not reconcile with Bronze.")

print("Cab-rides Silver persistence validation passed.")

# COMMAND ----------

# Transform and standardize the weather data

# Convert and standardize the Weather Bronze data
weather_standardized_df = (
    weather_bronze_df

        # Standardize location text
        .withColumn(
            "location",
            F.trim(F.col("location"))
        )

        # Convert the 10-digit timestamp expressed in seconds
        .withColumn(
            "weather_datetime_utc",
            F.expr("timestamp_seconds(time_stamp)")
        )

        # Convert UTC into Boston local time
        .withColumn(
            "weather_datetime_local",
            F.from_utc_timestamp(
                F.col("weather_datetime_utc"),
                "America/New_York"
            )
        )

        # Create Tableau-friendly date and time fields
        .withColumn(
            "weather_date_local",
            F.to_date(F.col("weather_datetime_local"))
        )
        .withColumn(
            "weather_hour_local",
            F.hour(F.col("weather_datetime_local"))
        )
        .withColumn(
            "weather_day_name_local",
            F.date_format(
                F.col("weather_datetime_local"),
                "EEEE"
            )
        )
        .withColumn(
            "weather_hour_start_local",
            F.date_trunc(
                "hour",
                F.col("weather_datetime_local")
            )
        )

        # Preserve whether rain was originally missing
        .withColumn(
            "rain_was_missing",
            F.col("rain").isNull()
        )

        # Treat an unreported rain measurement as zero
        .withColumn(
            "rain_amount",
            F.coalesce(
                F.col("rain"),
                F.lit(0.0)
            )
        )

        # Create an easy-to-use analytical rain indicator
        .withColumn(
            "is_raining",
            F.col("rain_amount") > 0
        )

        # Apply data-quality rules
        .withColumn(
            "quality_status",
            F.when(
                F.col("location").isNull() |
                (F.trim(F.col("location")) == ""),
                F.lit("INVALID_LOCATION")
            )
            .when(
                F.col("weather_datetime_utc").isNull(),
                F.lit("INVALID_TIMESTAMP")
            )
            .when(
                F.col("temp").isNull(),
                F.lit("MISSING_TEMPERATURE")
            )
            .when(
                F.col("humidity").isNull() |
                (F.col("humidity") < 0) |
                (F.col("humidity") > 1),
                F.lit("INVALID_HUMIDITY")
            )
            .when(
                F.col("clouds").isNull() |
                (F.col("clouds") < 0) |
                (F.col("clouds") > 1),
                F.lit("INVALID_CLOUD_COVER")
            )
            .otherwise(F.lit("VALID"))
        )

        # Record the Silver transformation time
        .withColumn(
            "_silver_transformed_at",
            F.current_timestamp()
        )
)

print("Weather standardized DataFrame created.")
print(
    f"Rows available for transformation: "
    f"{weather_standardized_df.count():,}"
)

display(
    weather_standardized_df.select(
        "location",
        "temp",
        "clouds",
        "pressure",
        "rain",
        "rain_was_missing",
        "rain_amount",
        "is_raining",
        "humidity",
        "wind",
        "weather_datetime_utc",
        "weather_datetime_local",
        "weather_date_local",
        "weather_hour_local",
        "weather_hour_start_local",
        "quality_status"
    ).limit(10)
)

# COMMAND ----------

# Validate the standardized weather DataFrame

# Display the weather quality-status distribution

weather_quality_summary_df = (
    weather_standardized_df
        .groupBy("quality_status")
        .agg(F.count("*").alias("row_count"))
        .orderBy("quality_status")
)

print("Weather quality-status distribution:")
display(weather_quality_summary_df)

# Calculate standardized weather validation metrics
weather_standardized_validation_df = (
    weather_standardized_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("location").alias("unique_locations"),
            F.countDistinct("location", "time_stamp").alias("unique_location_timestamp_keys"),
            F.sum(F.when(F.col("weather_datetime_utc").isNull(), 1).otherwise(0)).alias("null_utc_datetimes"),
            F.sum(F.when(F.col("weather_datetime_local").isNull(), 1).otherwise(0)).alias("null_local_datetimes"),
            F.sum(F.when(F.col("rain").isNull(), 1).otherwise(0)).alias("original_null_rain_rows"),
            F.sum(F.when(F.col("rain_amount").isNull(), 1).otherwise(0)).alias("null_rain_amount_rows"),
            F.sum(F.when(F.col("rain_was_missing") == True, 1).otherwise(0)).alias("rain_missing_flag_rows"),
            F.sum(F.when(F.col("quality_status") != "VALID", 1).otherwise(0)).alias("invalid_quality_rows"),
            F.min("weather_datetime_local").alias("minimum_local_datetime"),
            F.max("weather_datetime_local").alias("maximum_local_datetime")
        )
)

print("Weather standardized validation:")
display(weather_standardized_validation_df)

# Confirm that missing rain values became zero
rain_replacement_error_count = (
    weather_standardized_df
        .filter(
            F.col("rain").isNull() &
            (F.col("rain_amount") != 0.0)
        )
        .count()
)

# Check whether a location has multiple records for the same timestamp
duplicate_weather_keys_df = (
    weather_standardized_df
        .groupBy("location", "time_stamp")
        .agg(F.count("*").alias("record_count"))
        .filter(F.col("record_count") > 1)
)

duplicate_weather_key_count = duplicate_weather_keys_df.count()

print(
    f"Incorrectly replaced rain values: "
    f"{rain_replacement_error_count:,}"
)

print(
    f"Duplicate location-timestamp keys: "
    f"{duplicate_weather_key_count:,}"
)

# COMMAND ----------

# Write the clean Weather Silver table

# Keep only weather records that passed the Silver quality rules
weather_clean_df = (
    weather_standardized_df
        .filter(F.col("quality_status") == "VALID")
)

# Validate the record count before writing
weather_clean_count = weather_clean_df.count()

print(f"Clean weather rows: {weather_clean_count:,}")
print(f"Original Weather Bronze rows: {weather_bronze_count:,}")

# Stop execution if the weather clean record doesn't reconcile with the bronze table
if weather_clean_count != weather_bronze_count:
    raise ValueError(
        "Weather silver validation failed before writing. "
        f"Bronze rows: {weather_bronze_count:,}"
        f"Clean rows: {weather_clean_count:,}"
    )

# Write the clean weather data as a governed Delta table
(
    weather_clean_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(weather_silver_table)
)

print("Weather Silver write completed successfully.")
print(f"Silver table: {weather_silver_table}")

# COMMAND ----------

# Read the persisted Weather Silver Delta table
weather_clean_written_df = spark.table(weather_silver_table)

# Validate the persisted table
weather_clean_written_summary_df = (
    weather_clean_written_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("location").alias("unique_locations"),
            F.countDistinct("location", "time_stamp").alias("unique_location_timestamp_keys"),
            F.sum(F.when(F.col("quality_status") != "VALID", 1).otherwise(0)).alias("non_valid_rows"),
            F.sum(F.when(F.col("weather_datetime_local").isNull(), 1).otherwise(0)).alias("null_local_datetimes"),
            F.sum(F.when(F.col("rain").isNull(), 1).otherwise(0)).alias("original_null_rain_rows"),
            F.sum(F.when(F.col("rain_amount").isNull(), 1).otherwise(0)).alias("null_rain_amount_rows"),
            F.sum(F.when(F.col("rain_was_missing") == True, 1).otherwise(0)).alias("rain_missing_flag_rows"),
            F.min("weather_datetime_local").alias("minimum_local_datetime"),
            F.max("weather_datetime_local").alias("maximum_local_datetime")
        )
)

print("Persisted Weather Silver validation:")
display(weather_clean_written_summary_df)

# Check for duplicated business keys after persistence
persisted_duplicate_weaher_keys = (
    weather_clean_written_df
        .groupBy("location", "time_stamp")
        .agg(F.count("*").alias("record_count"))
        .filter(F.col("record_count") > 1)
        .count()
)

print(
    f"Persisted duplicate location-timestamp keys: "
    f"{persisted_duplicate_weaher_keys:,}"
)

# Display sample persisted records
display(
    weather_clean_written_df.select(
        "location",
        "temp",
        "clouds",
        "pressure",
        "rain",
        "rain_amount",
        "rain_was_missing",
        "is_raining",
        "humidity",
        "wind",
        "weather_datetime_local",
        "weather_hour_start_local",
        "quality_status"
    ).limit(10)
)

if weather_clean_written_df.count() != weather_bronze_count:
    raise ValueError(
        "Persisted Weather Silver row count does not match Bronze."
    )

print("Weather Silver persistence validation passed.")