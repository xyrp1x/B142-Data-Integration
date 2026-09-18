# NYC Yellow Taxi Data Integration with Apache Spark on Databricks

## 1. Introduction
The NYC Yellow Taxi dataset represents true big data—containing millions of trip records that capture passenger demand, distance travelled, fare amounts, tips, and location metadata. Processing, integrating, and extracting actionable insights from a dataset of this scale necessitates the use of distributed computation frameworks like **Apache Spark**.

This project demonstrates how to design and implement an end-to-end big data management pipeline on **Databricks** using the Spark API. The workflow encompasses the following critical phases:
* **Ingestion:** Reading raw multi-million row taxi trip records and corresponding zone lookup metadata.
* **Data Cleaning & Transformation:** Enhancing data quality by handling missing values and engineering precise features.
* **Data Integration:** Enriching individual trip records by aligning schemas and joining them with zone metadata.
* **ETL Pipeline:** Persisting clean fact tables and pre-aggregated analytical layers into managed Delta tables.
* **Analytics & Visualization:** Executing high-performance Spark SQL queries and generating programmatic visualizations.

## 2. System Design & Implementation Steps

### Step 1 & 2: Ingestion and Data Cleaning
* Raw data was ingested from managed tables in the Databricks catalog (`yellow_tripdata_2023_01` and `taxi_zones`).
* Standardized timestamps using `coalesce` into `pickup_ts` and `dropoff_ts`.
* Derived features: Trip distance in kilometers, trip duration in minutes, and average speed in km/h.
* Applied strict filters using the Spark DataFrame API to remove invalid records (e.g., negative fares, zero duration).

### Step 3: Integration with Zone Metadata
Individual trip records were enriched by performing a schema alignment and joining the fact dataset with lookup metadata. Each record was successfully mapped to include the specific borough and zone names for both pickup and drop-off locations.

### Step 4: Persisting Fact and Aggregate Tables
The curated fact layer and pre-aggregated analytical dimensions were saved as managed tables in the Databricks Catalog using the Delta Lake format. 
* `trips_yellow` (Curated core fact table)
* `agg_hourly` (Hourly demand and velocity aggregates)
* `agg_top_zones` (Revenue and trip density analytics by pickup location)

### Step 5: SQL Analytics
Using the managed Delta tables, distributed Spark SQL queries were executed to perform advanced data analysis and extract descriptive business insights, focusing on total revenue distribution by borough, average tip percentages, and speed percentiles.

## 3. Challenges and Solutions
* **DBFS Limitation:** Since DBFS was disabled, tables were managed directly within the default workspace catalog.
* **Data Quality Issues:** Anomalies like negative fares were resolved using strict filtering constraints.
* **Scalability (30M+ Rows):** Curated outputs were saved as optimized Delta tables to support high-performance analytical queries.


