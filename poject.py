# Databricks notebook source
# MAGIC %md Data Integration  Project
# MAGIC
# MAGIC Big Data Integration Processing for NYC Taxi Trip Analysis Using Apache Spark
# MAGIC
# MAGIC Name: Tymur Lukianov
# MAGIC Student ID: GH1026500
# MAGIC
# MAGIC 1)Introduction
# MAGIC
# MAGIC This project will demonstrate how big data integration and distributed processing can be applied to NYC Taxi Trip Data, a real-world large-scale dataset.
# MAGIC
# MAGIC The dataset contains millions of taxi rides with details such as:
# MAGIC
# MAGIC Pickup and drop-off timestamps
# MAGIC
# MAGIC Pickup and drop-off location zones
# MAGIC
# MAGIC Trip distances
# MAGIC
# MAGIC Payment types (cash, card, etc.)
# MAGIC
# MAGIC Fare amounts and surcharges
# MAGIC
# MAGIC
# MAGIC
# MAGIC The key objectives of  project are:
# MAGIC
# MAGIC To design and implement a data pipeline using Apache Spark on Databricks.
# MAGIC
# MAGIC To clean and preprocess raw taxi trip data.
# MAGIC
# MAGIC To integrate multiple sources (trip records + taxi zone lookup).
# MAGIC
# MAGIC To apply ETL processing (Extract → Transform → Load).
# MAGIC
# MAGIC To generate analytics and insights about taxi demand, trip distances, and revenue.
# MAGIC
# MAGIC To visualize results for decision-making support.
# MAGIC
# MAGIC
# MAGIC Data used:
# MAGIC Trips (Jan 2023): yellow_tripdata_2023_01
# MAGIC
# MAGIC Zone lookup: taxi_zones
# MAGIC
# MAGIC In my run the tables contained ~30,667,656 trip rows and 265 zone records 
# MAGIC
# MAGIC
# MAGIC 2)System Design
# MAGIC
# MAGIC The project i wrote in  a structured ETL pipeline:
# MAGIC
# MAGIC Key Components:
# MAGIC
# MAGIC Data Ingestion – Taxi trip data and taxi zone lookup file loaded into Spark.
# MAGIC
# MAGIC Data Cleaning – Handle missing values, casting data types, and ensuring schema consistency.
# MAGIC
# MAGIC Data Integration – Joining trip records with zone lookup to map pickup-dropoff location IDs
# MAGIC
# MAGIC Transformation – Deriving new features such as trip duration, average speed, and revenue per trip.
# MAGIC
# MAGIC Analytics – Used Spark SQL for aggregations, grouping, and statistical summaries.
# MAGIC
# MAGIC Visualization – Generated small but efficent charts for demand analysis and revenue distribution.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3) Implementation
# MAGIC Ingestion, cleaning, integration

# COMMAND ----------

trips = spark.table("workspace.default.yellow_tripdata_2023_01")
zones = spark.table("workspace.default.taxi_zone_lookup")

display(trips.limit(5))
display(zones.limit(5))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- list tables
# MAGIC SHOW TABLES IN workspace.default;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4) Analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.default;
# MAGIC

# COMMAND ----------

trips_raw = spark.table("workspace.default.yellow_tripdata_2023_01")   
zones     = spark.table("workspace.default.taxi_zone_lookup")          

display(trips_raw.limit(5))
display(zones.limit(5))
print(trips_raw.count(), zones.count())


# COMMAND ----------

from pyspark.sql import functions as F

def coalesce_col(df, names, alias):
    cols = [F.col(n) for n in names if n in df.columns]
    return df.withColumn(alias, F.coalesce(*cols)) if cols else df

df = trips_raw
df = coalesce_col(df, ["tpep_pickup_datetime","pickup_datetime","lpep_pickup_datetime"], "pickup_ts")
df = coalesce_col(df, ["tpep_dropoff_datetime","dropoff_datetime","lpep_dropoff_datetime"], "dropoff_ts")

km = F.col("trip_distance") * F.lit(1.60934)
dur_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))/60.0
speed_kmh = (km / (dur_min/60.0))

trips_clean = (df
    .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    .filter(F.col("trip_distance").isNotNull())
    .withColumn("trip_distance_km", km)
    .withColumn("trip_duration_min", dur_min)
    .withColumn("avg_speed_kmh", speed_kmh)
    .filter(F.col("trip_duration_min") > 0.5)
    .filter(F.col("trip_distance_km").between(0.05, 200.0))
    .filter(F.col("fare_amount") >= 0.0)
    .withColumn("year",  F.year("pickup_ts"))
    .withColumn("month", F.month("pickup_ts"))
    .withColumn("hour",  F.hour("pickup_ts"))
)
display(trips_clean.limit(10))


# COMMAND ----------

zones_sel = (zones
             .withColumn("LocationID", F.col("LocationID").cast("int"))
             .select("LocationID","Borough","Zone","service_zone"))

trips_int = (trips_clean
    .join(zones_sel.withColumnRenamed("Borough","pu_borough")
                   .withColumnRenamed("Zone","pu_zone")
                   .withColumnRenamed("service_zone","pu_service_zone"),
          trips_clean.PULocationID == zones_sel.LocationID, "left")
    .drop(zones_sel.LocationID)
)

trips_int = (trips_int
    .join(zones_sel.withColumnRenamed("Borough","do_borough")
                   .withColumnRenamed("Zone","do_zone")
                   .withColumnRenamed("service_zone","do_service_zone"),
          trips_int.DOLocationID == zones_sel.LocationID, "left")
    .drop(zones_sel.LocationID)
)

display(trips_int.limit(10))


# COMMAND ----------

# refresh facat table
(trips_int
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("workspace.default.trips_yellow"))

# aggregations
hourly = (trips_int.groupBy("year","month","hour")
          .agg(F.count("*").alias("trips"),
               F.avg("trip_distance_km").alias("avg_km"),
               F.avg("trip_duration_min").alias("avg_min"),
               F.avg("avg_speed_kmh").alias("avg_speed_kmh"),
               F.avg("tip_amount").alias("avg_tip"),
               F.avg("total_amount").alias("avg_total")))
(hourly.write.format("delta").mode("overwrite")
       .saveAsTable("workspace.default.agg_hourly"))

top_zones = (trips_int.groupBy("year","month","pu_borough","pu_zone")
             .agg(F.count("*").alias("trips"),
                  F.sum("total_amount").alias("sum_revenue"),
                  F.avg("tip_amount").alias("avg_tip")))
(top_zones.write.format("delta").mode("overwrite")
          .saveAsTable("workspace.default.agg_top_zones"))


# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.default;
# MAGIC
# MAGIC
# MAGIC SELECT COUNT(*) AS rows FROM trips_yellow;
# MAGIC
# MAGIC -- revenue by borough
# MAGIC SELECT year, month, pu_borough,
# MAGIC        COUNT(*) AS trips,
# MAGIC        ROUND(SUM(total_amount),2) AS revenue
# MAGIC FROM trips_yellow
# MAGIC GROUP BY year, month, pu_borough
# MAGIC ORDER BY year, month, revenue DESC;
# MAGIC
# MAGIC -- tip rate by rate code
# MAGIC SELECT year, month, RatecodeID,
# MAGIC        ROUND(AVG(CASE WHEN total_amount>0 THEN tip_amount/total_amount END), 4) AS avg_tip_rate
# MAGIC FROM trips_yellow
# MAGIC GROUP BY year, month, RatecodeID
# MAGIC ORDER BY year, month, avg_tip_rate DESC;
# MAGIC
# MAGIC -- speed distribution
# MAGIC SELECT year, month,
# MAGIC        approx_percentile(avg_speed_kmh, array(0.1,0.5,0.9)) AS speed_kmh_p10_p50_p90
# MAGIC FROM trips_yellow
# MAGIC GROUP BY year, month;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 6) Visualization

# COMMAND ----------

import matplotlib.pyplot as plt

hourly = spark.table("workspace.default.agg_hourly").toPandas().sort_values(["year","month","hour"])

plt.figure()
hourly.groupby("hour")["trips"].sum().plot(kind="line", marker="o", title="Trips by Hour")
plt.xlabel("Hour"); plt.ylabel("Trips"); plt.tight_layout()
display(plt.gcf())

plt.figure()
hourly.groupby("hour")["avg_speed_kmh"].mean().plot(kind="line", marker="o", title="Average Speed by Hour (km/h)")
plt.xlabel("Hour"); plt.ylabel("km/h"); plt.tight_layout()
display(plt.gcf())


# COMMAND ----------

# MAGIC %md
# MAGIC 7) Results of investigation
# MAGIC
# MAGIC The analysis produced several key insights:
# MAGIC
# MAGIC The highest number of trips originated from Manhattan, confirming it as the busiest taxi borough.
# MAGIC
# MAGIC Evening rush hours (5–8 PM) showed peak demand
# MAGIC
# MAGIC The average trip distance was around 2–3 miles, with longer trips going to outer boroughs and airports
# MAGIC
# MAGIC Credit card payments dominate over cash, showing customer preference trends
# MAGIC
# MAGIC Revenue distribution  reveals that airport trips generate higher average fares than short city rides