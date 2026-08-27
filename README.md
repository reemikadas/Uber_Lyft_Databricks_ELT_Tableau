# Uber_Lyft_Databricks_ELT_Tableau
Databricks ELT pipeline for Uber/Lyft ride and weather data with Tableau Public analytics.

## Pipeline Progress

### Bronze Layer — Completed

The Bronze layer incrementally ingests the original CSV files with Databricks Auto Loader and stores them as governed Delta tables in Unity Catalog.

#### Source files

- `cab_rides.csv`
- `weather.csv`

The source CSV files are excluded from GitHub through `.gitignore`.

#### Unity Catalog structure

- Catalog: `rideshare_elt`
- Bronze schema: `rideshare_elt.bronze`
- Silver schema: `rideshare_elt.silver`
- Gold schema: `rideshare_elt.gold`

#### Bronze Delta tables

- `rideshare_elt.bronze.cab_rides_raw`
- `rideshare_elt.bronze.weather_raw`

#### Bronze validation results

| Dataset | Rows | Rescued rows | Source files |
|---|---:|---:|---:|
| Cab rides | 693,071 | 0 | 1 |
| Weather | 6,276 | 0 | 1 |

Additional quality observations:

- Cab rides contain 55,095 records with a missing price.
- Weather contains 5,382 records with a missing rain measurement.
- Raw values remain unchanged in the Bronze layer.
- Auto Loader checkpoints prevent previously processed files from being ingested again.
- Ingestion timestamps and source-file metadata are included for traceability.

## Current Architecture

```text
CSV files
    |
    v
Databricks Volumes
    |
    v
Auto Loader
    |
    v
Bronze Delta Tables  <-- Completed
    |
    v
Silver Clean Tables  <-- Next
    |
    v
Gold Tableau Dataset
    |
    v
CSV Export
    |
    v
Tableau Public
