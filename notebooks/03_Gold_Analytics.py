# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics
# MAGIC
# MAGIC **Project:** Uber/Lyft Databricks ELT Pipeline  
# MAGIC **Source layer:** Silver  
# MAGIC **Target layer:** Gold  
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook combines clean Uber/Lyft fare quotes with weather data and creates a business-ready Gold dataset for Tableau and predictive machine learning.
# MAGIC
# MAGIC ### Fare quote and weather integration
# MAGIC
# MAGIC - Treat the source timestamp as the time when the fare data was queried.
# MAGIC - Match each fare quote with the nearest weather observation at its pickup location.
# MAGIC - Match each fare quote with the nearest weather observation at its destination.
# MAGIC - Use the fare-query timestamp for both weather matches.
# MAGIC - Use weather-match timestamps and time differences for validation.
# MAGIC - Maintain exactly one Gold record per valid fare quote.
# MAGIC
# MAGIC ### Business enrichments
# MAGIC
# MAGIC - Create a route field and retain provider/product attributes.
# MAGIC - Retain query-time, distance, price, and surge fields.
# MAGIC - Add pickup and destination weather features.
# MAGIC - Validate weather-match quality before publishing Gold.
# MAGIC - Create Tableau-friendly query date and time fields.
# MAGIC
# MAGIC ### Gold output
# MAGIC
# MAGIC - `rideshare_elt.gold.rides_weather_enriched`
# MAGIC
# MAGIC ### Downstream consumers
# MAGIC
# MAGIC - Tableau Public fare-analysis dashboard
# MAGIC - Ride-fare predictive ML model
# MAGIC
# MAGIC ### Expected data grain
# MAGIC
# MAGIC One row per valid fare quote.
# MAGIC
# MAGIC Expected row count:
# MAGIC
# MAGIC `637,976`

# COMMAND ----------

# Configure Gold and load the Silver tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Interpret and display timestamps consistently
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Silver Input Tables
cab_silver_table = "rideshare_elt.silver.cab_rides_clean"
weather_silver_table = "rideshare_elt.silver.weather_clean"

# Gold Output Table
gold_enriched_table = "rideshare_elt.gold.rides_weather_enriched"

# Expected Silver row counts
expected_cab_silver_count = 637_976
expected_weather_silver_count = 6_276

# Confirm that both required Silver tables exist
required_silver_table = [
    cab_silver_table,
    weather_silver_table
]

missing_tables = [
    table_name
    for table_name in required_silver_table
    if not spark.catalog.tableExists(table_name)
]

if missing_tables:
    raise ValueError(
        f"Required Silver tables were not found: {missing_tables}"
    )

# Load the Silver Delta tables
cab_silver_df = spark.table(cab_silver_table)
weather_silver_df = spark.table(weather_silver_table)

# Count the input records
cab_silver_count = cab_silver_df.count()
weather_silver_count = weather_silver_df.count()

print("Gold notebook configuration completed.")
print(f"Cab-rides Silver rows: {cab_silver_count:,}")
print(f"Weather Silver rows: {weather_silver_count:,}")
print(f"Gold target table: {gold_enriched_table}")

# Stop if the inputs do not match with the validated Silver counts
if cab_silver_count != expected_cab_silver_count:
    raise ValueError(
        "Unexpected cab-rides Silver count."
        f"Expected {expected_cab_silver_count:,}, "
        f"but found {cab_silver_count:,}."    
    )

if weather_silver_count != expected_weather_silver_count:
    raise ValueError(
        "Unexpected weather Silver count."
        f"Expected {expected_weather_silver_count:,}, "
        f"but found {weather_silver_count:,}."    
    )

print("Silver input validation passed.")

# COMMAND ----------

# Profile weather timing and location coverage

# Window used to compare consecutive weather observations
weather_time_window = (
    Window
        .partitionBy("location")
        .orderBy("weather_datetime_utc")
)

# Calculate the interval between consecutive observations
weather_intervals_df = (
    weather_silver_df
        .select(
            "location",
            "weather_datetime_utc"
        )
        .withColumn(
            "previous_weather_datetime_utc",
            F.lag("weather_datetime_utc").over(weather_time_window)
        )
        .withColumn(
            "interval_minutes",
            (
                F.col("weather_datetime_utc").cast("long") -
                F.col("previous_weather_datetime_utc").cast("long")
            ) / 60.0
        )
        .filter(
            F.col("previous_weather_datetime_utc").isNotNull()
        )
)

# Summarize observation intervals for each location
weather_interval_summary_df = (
    weather_intervals_df
        .groupBy("location")
        .agg(
            F.count("*").alias("interval_count"),
            F.round(
                F.avg("interval_minutes"), 2
            ).alias("average_interval_minutes"),
            F.round(
                F.expr(
                    "percentile_approx(interval_minutes, 0.5)"
                ), 2
            ).alias("median_interval_minutes"),
            F.round(
                F.expr(
                    "percentile_approx(interval_minutes, 0.9)"
                ), 2
            ).alias("p90_interval_minutes"),
            F.round(
                F.max("interval_minutes"), 2
            ).alias("maximum_interval_minutes")
        )
        .orderBy("location")
)

print("Weather observation intervals by location:")
display(weather_interval_summary_df)

# Create one list containing all pickup and destination locations
ride_locations_df = (
    cab_silver_df
        .select(
            F.col("source").alias("location")
        )
        .union(
            cab_silver_df.select(
                F.col("destination").alias("location")
            )
        )
        .distinct()
)

weather_locations_df = (
    weather_silver_df
        .select("location")
        .distinct()
)

# Find ride locations that are absent from weather Silver
unmatched_ride_locations_df = (
    ride_locations_df
        .join(
            weather_locations_df,
            on="location",
            how="left_anti"
        )
)

ride_location_count = ride_locations_df.count()
weather_location_count = weather_locations_df.count()
unmatched_location_count = unmatched_ride_locations_df.count()

print(f"Distinct ride locations: {ride_location_count}")
print(f"Distinct weather locations: {weather_location_count}")
print(f"Ride locations without weather data: {unmatched_location_count}")

if unmatched_location_count > 0:
    print("Unmatched ride locations:")
    display(unmatched_ride_locations_df)

# Compare fare-query and weather time ranges
fare_query_range_df = (
    cab_silver_df
        .agg(
            F.min("ride_datetime_utc").alias("minimum_fare_query_datetime_utc"),
            F.max("ride_datetime_utc").alias("maximum_fare_query_datetime_utc")
        )
)

weather_range_df = (
    weather_silver_df
        .agg(
            F.min("weather_datetime_utc").alias("minimum_weather_datetime_utc"),
            F.max("weather_datetime_utc").alias("maximum_weather_datetime_utc")
        )
)

print("Fare-query timestamp range:")
display(fare_query_range_df)

print("Weather timestamp range:")
display(weather_range_df)

# COMMAND ----------

# Match the nearest source-location weather

# Maximum acceptable difference between a fare query and weather reading
weather_tolerance_minutes = 60
weather_tolerance_seconds = weather_tolerance_minutes * 60

# Use business-friendly query terminology in the Gold Layer
cab_gold_base_df = (
    cab_silver_df
        .withColumnRenamed("ride_datetime_utc", "query_datetime_utc")
        .withColumnRenamed("ride_datetime_local", "query_datetime_local")
        .withColumnRenamed("ride_date_local", "query_date_local")
        .withColumnRenamed("ride_hour_local", "query_hour_local")
        .withColumnRenamed("ride_day_name_local", "query_day_name_local")
)

# Select and rename weather fields for the source/pickup location
source_weather_df = (
    weather_silver_df
        .select(
            F.col("location").alias("source_weather_location"),
            F.col("weather_datetime_utc").alias("source_weather_datetime_utc"),
            F.col("temp").alias("source_temperature"),
            F.col("clouds").alias("source_clouds"),
            F.col("pressure").alias("source_pressure"),
            F.col("rain_amount").alias("source_rain_amount"),
            F.col("humidity").alias("source_humidity"),
            F.col("wind").alias("source_wind"),
            F.col("is_raining").alias("source_is_raining")
        )
)

# Find weather readings at the same source location
# within 60 minutes before/after the fare query

source_weather_candidates_df = (
    cab_gold_base_df.alias("cab")
        .join(
            source_weather_df.alias("weather"),
            (
                F.col("cab.source") == F.col("weather.source_weather_location")
            )
            &
            (
                F.abs(
                    F.col("cab.query_datetime_utc").cast("long") -
                    F.col("weather.source_weather_datetime_utc").cast("long")
                ) <= weather_tolerance_seconds
            ),
            how="left"
        )
        .withColumn(
            "source_weather_difference_minutes",
            F.round(
                F.abs(
                    F.col("query_datetime_utc").cast("long") -
                    F.col("source_weather_datetime_utc").cast("long")
                ) / 60.0,
                2
            )
        )
)

# Rank candidate weather readings by their distance from the query time
source_weather_rank_window = (
    Window
        .partitionBy("id")
        .orderBy(
            F.col("source_weather_difference_minutes").asc_nulls_last(),
            F.col("source_weather_datetime_utc").desc_nulls_last()
        )
)

# Keep only the nearest source weather reading for each fare quote
cab_with_source_weather_df = (
    source_weather_candidates_df
        .withColumn(
            "source_weather_rank",
            F.row_number().over(source_weather_rank_window)
        )
        .filter(F.col("source_weather_rank") == 1)
        .drop("source_weather_rank")
)

print("Nearest source-location weather matching completed.")
print(f"Weather tolerance: {weather_tolerance_minutes} minutes.")
print(f"Output rows: {cab_with_source_weather_df.count():,}")

display(
    cab_with_source_weather_df.select(
        "id",
        "source",
        "query_datetime_utc",
        "source_weather_datetime_utc",
        "source_weather_difference_minutes",
        "source_temperature",
        "source_rain_amount"
    ).limit(20)
)

# COMMAND ----------

# Validate source-weather matching
source_weather_validation_df = (
    cab_with_source_weather_df
        .agg(
            F.count("*").alias("total_fare_quotes"),
            F.countDistinct("id").alias("unique_fare_quote_ids"),
            F.sum(F.when(F.col("source_weather_datetime_utc").isNotNull(), 1).otherwise(0)).alias("matched_source_weather_quotes"),
            F.sum(F.when(F.col("source_weather_datetime_utc").isNull(), 1).otherwise(0)).alias("unmatched_source_weather_quotes"),
            F.round(
                100.0 *
                F.sum(F.when(F.col("source_weather_datetime_utc").isNotNull(), 1).otherwise(0))/ F.count("*"), 2
                ).alias("source_weather_match_rate_percent"),
            F.round(F.avg("source_weather_difference_minutes"), 2).alias("average_difference_minutes"),
            F.round(F.max("source_weather_difference_minutes"), 2).alias("maximum_difference_minutes"),
            F.sum(F.when(
                F.col("source_weather_difference_minutes") > weather_tolerance_minutes,1).otherwise(0)).alias("tolerance_violations")
        )
)

display(source_weather_validation_df)

# COMMAND ----------

source_validation = source_weather_validation_df.first()

assert source_validation["total_fare_quotes"] == expected_cab_silver_count, \
    "The source-weather join changed the fare-quote row count."

assert source_validation["unique_fare_quote_ids"] == expected_cab_silver_count, \
    "Duplicate or missing fare-quote IDs were detected."

assert source_validation["tolerance_violations"] == 0, \
    "A weather match exceeded the 60-minute tolerance."

print("Source-weather matching validation passed.")
print(f"Total fare quotes: {source_validation["total_fare_quotes"]:,}")
print(f"Matched source weather: {source_validation["matched_source_weather_quotes"]:,}")
print(f"Unmatched source weather: {source_validation["unmatched_source_weather_quotes"]:,}")
print(f"Source-weather match rate: {source_validation["source_weather_match_rate_percent"]}%")

# COMMAND ----------

# Match destination-location weather

# Select and rename weather fields for the destination location
destination_weather_df = (
    weather_silver_df
        .select(
            F.col("location").alias("destination_weather_location"),
            F.col("weather_datetime_utc").alias("destination_weather_datetime_utc"),
            F.col("temp").alias("destination_temperature"),
            F.col("clouds").alias("destination_clouds"),
            F.col("pressure").alias("destination_pressure"),
            F.col("rain_amount").alias("destination_rain_amount"),
            F.col("humidity").alias("destination_humidity"),
            F.col("wind").alias("destination_wind"),
            F.col("is_raining").alias("destination_is_raining")
        )
)

# Find destination weather readings within 60 minutes
# before or after the fare-query timestamp
destination_weather_candidates_df = (
    cab_with_source_weather_df.alias("cab")
        .join(
            destination_weather_df.alias("weather"),
            (
                F.col("cab.destination") ==
                F.col("weather.destination_weather_location")
            )
            &
            (
                F.abs(
                    F.col("cab.query_datetime_utc").cast("long") -
                    F.col("weather.destination_weather_datetime_utc").cast("long")
                ) <= weather_tolerance_seconds
            ),
            how="left"
        )
        .withColumn(
            "destination_weather_difference_minutes",
            F.round(
                F.abs(
                    F.col("query_datetime_utc").cast("long") -
                    F.col("destination_weather_datetime_utc").cast("long")
                ) / 60.0, 2
            )
        )
)

# Rank destination weather readings from nearest to farthest
destination_weather_rank_window = (
    Window
        .partitionBy("id")
        .orderBy(
            F.col("destination_weather_difference_minutes").asc_nulls_last(),
            F.col("destination_weather_datetime_utc").desc_nulls_last()
        )
)

# Retain only the nearest destination weather reading
cab_with_both_weather_df = (
    destination_weather_candidates_df
        .withColumn(
            "destination_weather_rank",
            F.row_number().over(destination_weather_rank_window)
        )
        .filter(F.col("destination_weather_rank") == 1)
        .drop("destination_weather_rank")
)

destination_match_output_count = cab_with_both_weather_df.count()

print("Nearest destination-location weather matching completed.")
print(f"Weather tolerance: {weather_tolerance_minutes} minutes")
print(f"Output rows: {destination_match_output_count:,}")

display(
    cab_with_both_weather_df.select(
        "id",
        "source",
        "destination",
        "query_datetime_utc",
        "source_weather_datetime_utc",
        "source_weather_difference_minutes",
        "destination_weather_datetime_utc",
        "destination_weather_difference_minutes",
        "destination_temperature",
        "destination_rain_amount"
    ).limit(20)
)

# COMMAND ----------

# Validate destination-weather matching
destination_weather_validation_df = (
    cab_with_both_weather_df
        .agg(
            F.count("*").alias("total_fare_quotes"),
            F.countDistinct("id").alias("unique_fare_quote_ids"),
            F.sum(F.when(F.col("destination_weather_datetime_utc").isNotNull(), 1).otherwise(0)).alias("matched_destination_weather_quotes"),
            F.sum(F.when(F.col("destination_weather_datetime_utc").isNull(), 1).otherwise(0)).alias("unmatched_destination_weather_quotes"),
            F.round(
                100.0 *
                F.sum(F.when(F.col("destination_weather_datetime_utc").isNotNull(), 1).otherwise(0)) / F.count("*"), 2
            ).alias("destination_weather_match_rate_percent"),
            F.round(F.avg("destination_weather_difference_minutes"), 2).alias("average_difference_minutes"),
            F.round(F.max("destination_weather_difference_minutes"), 2).alias("maximum_difference_minutes"),
            F.sum(F.when(F.col("destination_weather_difference_minutes") > weather_tolerance_minutes, 1).otherwise(0)).alias("tolerance_violations"),
            F.sum(F.when(
                F.col("source_weather_datetime_utc").isNotNull()
                &
                F.col("destination_weather_datetime_utc").isNotNull(), 1
            ).otherwise(0)).alias("quotes_with_both_weather_matches")
        )
)

display(destination_weather_validation_df)

# COMMAND ----------

destination_validation = destination_weather_validation_df.first()

assert destination_validation["total_fare_quotes"] == expected_cab_silver_count, \
    "The destination-weather join changed the fare-quote row count."

assert destination_validation["unique_fare_quote_ids"] == expected_cab_silver_count, \
    "Duplicate or missing fare-quote IDs were detected."

assert destination_validation["tolerance_violations"] == 0, \
    "A destination-weather match exceeded the 60-minute tolerance."

print("Destination-weather matching validation passed.")
print(f"Total fare quotes: {destination_validation['total_fare_quotes']:,}")
print(f"Matched destination weather: {destination_validation['matched_destination_weather_quotes']:,}")
print(f"Unmatched destination weather: {destination_validation['unmatched_destination_weather_quotes']:,}")
print(f"Destination-weather match rate: {destination_validation['destination_weather_match_rate_percent']}%")
print(f"Quotes with both weather matches: {destination_validation['quotes_with_both_weather_matches']:,}")

# COMMAND ----------

# Create the final Gold DataFrame

gold_rides_weather_df = (
    cab_with_both_weather_df

        # Remove duplicated location fields created during weather matching
        .drop("source_weather_location",
              "destination_weather_location"
        )

        # Create a readable route for Tableau
        .withColumn(
            "route",
            F.concat_ws(" → ", F.col("source"), F.col("destination"))
        )

        # Calculate the average temperature across the route locations
        .withColumn(
            "average_route_temperature",
            F.round(
                (F.col("source_temperature") +
                 F.col("destination_temperature")
                ) / 2.0,
                2
            )
        )

        # Calculate the temperature difference between locations
        .withColumn(
            "route_temperature_difference",
            F.round(
                F.abs(
                    F.col("source_temperature") -
                    F.col("destination_temperature")
                ),
                2
            )
        )

        # Flag whether rain was present at either location
        .withColumn(
            "rain_at_either_location",
            F.coalesce(F.col("source_is_raining"), F.lit(False)) |
            F.coalesce(F.col("destination_is_raining"), F.lit(False))
        )

        # Record whether both weather matches were successful
        .withColumn(
            "weather_match_status",
            F.when(
                F.col("source_weather_datetime_utc").isNotNull()
                &
                F.col("destination_weather_datetime_utc").isNotNull(),
                F.lit("MATCHED")
            ).otherwise(F.lit("UNMATCHED"))
        )

        # Document the matching rule used to create Gold
        .withColumn(
            "weather_tolerance_minutes",
            F.lit(weather_tolerance_minutes)
        )

        # Record when the Gold record was created
        .withColumn(
            "_gold_created_at",
            F.current_timestamp()
        )

)

gold_dataframe_count = gold_rides_weather_df.count()

print("Final Gold DataFrame created.")
print(f"Gold rows: {gold_dataframe_count:,}")
print(f"Gold columns: {len(gold_rides_weather_df.columns)}")

display(
    gold_rides_weather_df.select(
        "id",
        "cab_type",
        "name",
        "source",
        "destination",
        "route",
        "distance",
        "price",
        "surge_multiplier",
        "query_datetime_local",
        "source_temperature",
        "destination_temperature",
        "average_route_temperature",
        "source_rain_amount",
        "destination_rain_amount",
        "rain_at_either_location",
        "weather_match_status",
        "weather_tolerance_minutes"
    ).limit(20)
)

# COMMAND ----------

# Create the curated Gold DataFrame
curated_gold_df = (
    gold_rides_weather_df
        .select(
            # Fare-quotes identifiers and service
            "id",
            "product_id",
            "cab_type",
            "name",

            # Locations
            "source",
            "destination",
            "route",

            # Fare and distance measures
            "distance",
            "price",
            "surge_multiplier",
            "price_per_mile",
            "surge_applied",

            # Query date and time
            "query_datetime_utc",
            "query_datetime_local",
            "query_date_local",
            "query_hour_local",
            "query_day_name_local",
            "is_weekend",

            # Source-location weather
            "source_temperature",
            "source_clouds",
            "source_pressure",
            "source_rain_amount",
            "source_humidity",
            "source_wind",
            "source_is_raining",

            # Destination-location weather
            "destination_temperature",
            "destination_clouds",
            "destination_pressure",
            "destination_rain_amount",
            "destination_humidity",
            "destination_wind",
            "destination_is_raining",

            # Business-friendly weather summaries
            F.col("average_route_temperature").alias("average_endpoint_temperature"),
            F.col("route_temperature_difference").alias("endpoint_temperature_difference"),

            "rain_at_either_location",

            # Gold refresh metadata
            "_gold_created_at"
        )
)

curated_gold_count = curated_gold_df.count()

print("Curated Gold DataFrame created.")
print(f"Rows: {curated_gold_count:,}")
print(f"Columns: {len(curated_gold_df.columns)}")

# COMMAND ----------

curated_gold_df.printSchema()

# COMMAND ----------

# Publish the curated Gold DataFrame as the final Gold Delta table
expected_curated_gold_column_count = 36

# Validate before overwriting the permanent table
if curated_gold_count != expected_cab_silver_count:
    raise ValueError(
        f"Expected {expected_cab_silver_count:,} rows, but found {curated_gold_count:,}."
    )

if len(curated_gold_df.columns) != expected_curated_gold_column_count:
    raise ValueError(
        f"Expected {expected_curated_gold_column_count} columns, but found {len(curated_gold_df.columns)}."
    )

# Make the curated DataFrame available to Spark SQL
curated_gold_df.createOrReplaceTempView(
    "curated_gold_staging_view"
)

# Explicitly replace the Delta table and its schema
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {gold_enriched_table}
    USING DELTA
    AS
    SELECT *
    FROM curated_gold_staging_view
    """
)

# Read the replaced table from Unity Catalog
stored_curated_gold_df = spark.table(gold_enriched_table)

stored_curated_row_count = stored_curated_gold_df.count()
stored_curated_column_count = len(stored_curated_gold_df.columns)

print("Curated Gold table replaced successfully.")
print(f"Gold table: {gold_enriched_table}")
print(f"Rows: {stored_curated_row_count:,}")
print(f"Columns: {stored_curated_column_count}")