# B142-Data-Integration
my final project
# 🚕 NYC Yellow Taxi Data Integration with Apache Spark on Databricks

## 1. Introduction

### Why is big data important for city transport?
The dataset used in this project, NYC Yellow Taxi, represents true big data—containing millions of trip records that capture passenger demand, distance travelled, fare amounts, tips, and location metadata. Processing, integrating, and extracting actionable insights from a dataset of this scale necessitates the use of distributed computation frameworks like **Apache Spark**.

This project demonstrates how to design and implement an end-to-end big data management pipeline on **Databricks** using the Spark API. The workflow encompasses the following critical phases:

*   **Ingestion:** Reading raw multi-million row taxi trip records and corresponding zone lookup metadata.
*   **Data Cleaning & Transformation:** Enhancing data quality by handling missing values and engineering precise features (e.g., converting distances to kilometers and calculating durations).
*   **Data Integration:** Enriching individual trip records by aligning schemas and joining them with pickup and drop-off zone metadata.
*   **ETL Pipeline:** Persisting clean fact tables and pre-aggregated analytical layers into managed Delta tables for optimized access.
*   **Analytics & Visualization:** Executing high-performance Spark SQL queries and generating programmatic visualizations to analyze trends like hourly demand spikes and traffic velocity patterns.

By completing these phases, this work demonstrates how Spark's distributed architecture efficiently supports the full ETL lifecycle for large-scale urban data integration.
