# Car Sales Data Engineering Pipeline

 ## Overview

An end-to-end data engineering project built using Azure Data Lake Storage Gen2 and Azure Databricks to process car sales data through Bronze, Silver, and Gold layers.

The project covers data transformation, aggregation, incremental processing, dimensional modelling, surrogate key generation, and Databricks Job orchestration.

## Technologies

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- SQL
- Parquet
- Delta Lake
- Unity Catalog
- Databricks Jobs

 ## Pipeline

 ADLS Gen2 → Bronze → Silver → Gold

 ### Silver Layer

- Read data from the Bronze container.
- Performed data transformations and aggregations using Databricks.
- Stored processed data in Parquet format in the Silver container.
- Queried the processed Silver data.

 ### Gold Layer

- Implemented initial and incremental processing using a flag parameter.
- Created dimension tables:
  - "dim_model"
  - "dim_branch"
  - "dim_date"
  - "dim_dealer"
- Generated surrogate keys for the dimension tables.
- Created the "fact_sales" table.
- Stored the Gold tables in the Gold schema.
- Stored the final "fact_sales" table in Delta format.

### Orchestration

Created a Databricks Job to execute and manage the pipeline workflow and task dependencies.

## Data Model

dim_model
     │
dim_branch ──► fact_sales ◄── dim_dealer
     │
  dim_date

## Key Concepts Demonstrated

- Azure Data Lake Storage
- Azure Databricks
- ETL/Data transformation
- Bronze-Silver-Gold architecture
- Incremental processing
- Dimensional modelling
- Surrogate keys
- Fact & dimension tables
- Parquet and Delta formats
- Databricks Job orchestration

## Project Screenshots

Screenshots of the Databricks Job, successful pipeline execution, and Gold-layer tables are included in this repository.

## Future Enhancements

- Implement CI/CD using GitHub.
- Add Databricks Asset Bundles for deployment.
- Add data quality checks and pipeline monitoring.
- Connect the Gold layer to a BI dashboard.
- Add Databricks Asset Bundles for deployment.
- Add data quality checks and pipeline monitoring.
- Connect the Gold layer to a BI dashboard.
