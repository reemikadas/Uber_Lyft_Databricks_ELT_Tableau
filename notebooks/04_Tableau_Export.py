# Databricks notebook source
# MAGIC %md
# MAGIC # Tableau Export
# MAGIC
# MAGIC **Project:** Uber/Lyft Databricks ELT Pipeline  
# MAGIC **Source layer:** Gold  
# MAGIC **Target:** Tableau Public CSV
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC - Load the curated Gold Delta table.
# MAGIC - Validate the dataset before export.
# MAGIC - Create a single analytics-ready CSV file.
# MAGIC - Store the CSV in a governed Unity Catalog volume.
# MAGIC - Download the CSV for use in Tableau Public.
# MAGIC
# MAGIC ## Source Table
# MAGIC
# MAGIC `rideshare_elt.gold.rides_weather_enriched`
# MAGIC
# MAGIC ## Expected Output
# MAGIC
# MAGIC - Rows: `637,976`
# MAGIC - Columns: `36`
# MAGIC - Grain: One row per valid fare quote

# COMMAND ----------

# Load and validate the Gold table
# Configure and load the curated Gold table

# Keep timestamp display consistent during the export
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Gold Source Table
gold_table = "rideshare_elt.gold.rides_weather_enriched"

# Expected dataset dimensions
expected_gold_rows = 637_976
expected_gold_columns = 36

# Confirm that the gold table exists
if not spark.catalog.tableExists(gold_table):
    raise ValueError(f"Required Gold table was not found: {gold_table}")

# Load the Gold Delta table
tableau_exports_df = spark.table(gold_table)

# Calculate its dimensions
actual_gold_rows = tableau_exports_df.count()
actual_gold_columns = len(tableau_exports_df.columns)

# Stop execution if the dimensions are unexpected
if actual_gold_rows != expected_gold_rows:
    raise ValueError(f"Expected {expected_gold_rows:,} gold rows, "
                     f"but found {actual_gold_rows:,}.")

if actual_gold_columns != expected_gold_columns:
    raise ValueError(f"Expected {expected_gold_columns} gold columns, "
                     f"but found {actual_gold_columns}.")

print("Gold table loaded and validated.")
print(f"Source table: {gold_table}")
print(f"Rows: {actual_gold_rows:,}")
print(f"Columns: {actual_gold_columns}")

display(tableau_exports_df.limit(10))

# COMMAND ----------

# Create the Tableau-ready DataFrame
from pyspark.sql import functions as F

expected_tableau_rows = 637_976
expected_tableau_columns = 35

# Remove pipeline metadata that is not needed in Tableau
tableau_ready_df = tableau_exports_df.drop("_gold_created_at")

# Validate the Tableau-ready dataset
tableau_validation = (
    tableau_ready_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("id").alias("unique_ids"),
            F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price_rows"),
            F.sum(F.when(F.col("source_temperature").isNull(), 1).otherwise(0)).alias("null_source_weather_rows"),
            F.sum(F.when(F.col("destination_temperature").isNull(), 1).otherwise(0)).alias("null_destination_weather_rows")
        )
        .first()
)

tableau_row_count = tableau_validation["total_rows"]
tableau_unique_id_count = tableau_validation["unique_ids"]
tableau_column_count = len(tableau_ready_df.columns)

# Stop if the Tableau dataset is unexpected
if tableau_row_count != expected_tableau_rows:
    raise ValueError(
        f"Expected {expected_tableau_rows:,} rows, "
        f"but found {tableau_row_count:,}."
    )

if tableau_unique_id_count != expected_tableau_rows:
    raise ValueError(
        "Duplicate or missing fare-quote IDs were detected."
    )

if tableau_column_count != expected_tableau_columns:
    raise ValueError(
        f"Expected {expected_tableau_columns} columns, "
        f"but found {tableau_column_count}."
    )

if tableau_validation["null_price_rows"] != 0:
    raise ValueError("The Tableau dataset contains null prices.")

if tableau_validation["null_source_weather_rows"] != 0:
    raise ValueError("The Tableau dataset contains unmatched source weather.")

if tableau_validation["null_destination_weather_rows"] != 0:
    raise ValueError(
        "The Tableau dataset contains unmatched destination weather."
    )

print("Tableau-ready DataFrame validation passed.")
print(f"Rows: {tableau_row_count:,}")
print(f"Unique IDs: {tableau_unique_id_count:,}")
print(f"Columns: {tableau_column_count}")
print("Excluded column: _gold_created_at")

display(tableau_ready_df.limit(10))

# COMMAND ----------

# Confirm the Tableau export volume exists

export_catalog = "rideshare_elt"
export_schema = "gold"
export_volume = "tableau_exports"

export_volume_path = (
    f"/Volumes/{export_catalog}/{export_schema}/{export_volume}"
)

available_gold_volumes_df = spark.sql(
    f"SHOW VOLUMES IN {export_catalog}.{export_schema}"
)

display(available_gold_volumes_df)

volume_exists = (
    available_gold_volumes_df
        .filter(f"volume_name = '{export_volume}'")
        .count() == 1
)

if not volume_exists:
    raise ValueError(f"Required export volume was not found: "
                     f"{export_catalog}.{export_schema}.{export_volume}")

print("Tableau export volume confirmed.")
print(f"Volume: {export_catalog}.{export_schema}.{export_volume}")
print(f"Path: {export_volume_path}")

# COMMAND ----------

# Export the Tableau-ready DataFrame as one named CSV file
export_file_name = "rideshare_gold.csv"

# Spark first writes the CSV into a temporary folder
temporary_export_path = (
    f"{export_volume_path}/_temporary_tableau_export"
)

# Final file location in the governed volume
final_export_path = (
    f"{export_volume_path}/{export_file_name}"
)

# Remove only previous versions of these specific export paths
dbutils.fs.rm(temporary_export_path, recurse=True)
dbutils.fs.rm(final_export_path, recurse=True)

# Coalesce(1) produces one CSV part file
(
    tableau_ready_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
        .option("nullValue", "")
        .csv(temporary_export_path)
)

# Find the CSV part file created by Spark
csv_part_files = [
    file.path
    for file in dbutils.fs.ls(temporary_export_path)
    if file.name.endswith(".csv")
]

if len(csv_part_files) != 1:
    raise ValueError(
        f"Expected exactly one CSV file, but found {len(csv_part_files)}."
    )

# Move and rename the Spark part file
dbutils.fs.mv(
    csv_part_files[0],
    final_export_path
)

# Remove the temporary folder and its _SUCCESS file
dbutils.fs.rm(temporary_export_path, recurse=True)

# Confirm that the final file exists
final_file_matches = [
    file
    for file in dbutils.fs.ls(export_volume_path)
    if file.name == export_file_name
]

if len(final_file_matches) != 1:
    raise ValueError(
        f"Final Tableau CSV was not found: {final_export_path}"
    )

final_file_size_mb = (
    final_file_matches[0].size / (1024 * 1024)
)

print("Tableau CSV export completed.")
print(f"File: {export_file_name}")
print(f"Path: {final_export_path}")
print(f"Rows exported: {tableau_row_count:,}")
print(f"Columns exported: {tableau_column_count}")
print(f"File size: {final_file_size_mb:,.2f} MB")

# COMMAND ----------

# Read the exported CSV back and validate its contents

exported_csv_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(final_export_path)
)

csv_validation = (
    exported_csv_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("id").alias("unique_ids")
        )
        .first()
)

exported_row_count = csv_validation["total_rows"]
exported_unique_id_count = csv_validation["unique_ids"]
exported_column_count = len(exported_csv_df.columns)

# Confirm that the CSV header matches the Tableau DataFrame
if exported_csv_df.columns != tableau_ready_df.columns:
    raise ValueError(
        "The exported CSV columns do not match "
        "the Tableau-ready DataFrame."
    )

if exported_row_count != expected_tableau_rows:
    raise ValueError(
        f"Expected {expected_tableau_rows:,} exported rows, "
        f"but found {exported_row_count:,}."
    )

if exported_unique_id_count != expected_tableau_rows:
    raise ValueError(
        "The exported CSV contains duplicate or missing IDs."
    )

if exported_column_count != expected_tableau_columns:
    raise ValueError(
        f"Expected {expected_tableau_columns} exported columns, "
        f"but found {exported_column_count}."
    )

print("Exported Tableau CSV validation passed.")
print(f"Rows: {exported_row_count:,}")
print(f"Unique IDs: {exported_unique_id_count:,}")
print(f"Columns: {exported_column_count}")

display(exported_csv_df.limit(10))