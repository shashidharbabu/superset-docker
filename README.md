# WAU Chart Assignment

This repository contains the code for a full data pipeline project that demonstrates the following:

1. **ETL Process:** Import two tables (`user_session_channel` and `session_timestamp`) from Snowflake into Airflow (using the `raw_data` schema).
2. **ELT Process:** Create an Airflow DAG to join these tables into a new table called `session_summary` (under the `analytics` schema) with an extra condition to remove duplicate records.
3. **BI Visualization:** Set up a BI tool (Preset, Docker Superset, or Tableau) to connect to Snowflake, import the `session_summary` table, and create a WAU (Weekly Active User) chart with the metric field renamed to "WAU".

## Repository Structure

```
├── dags
│   ├── etl_import_dag.py      # DAG for importing data from Snowflake
│   └── elt_join_dag.py        # DAG for creating the session_summary table
├── README.md                  # This file
```

## Prerequisites

- Apache Airflow (configured with a Snowflake connection named `snowflake_conn`)
- Snowflake account and credentials
- BI tool of your choice (Preset, Docker Superset, or Tableau) for data visualization
- Git

## How to Run

1. **Airflow Setup:**
   - Place the DAG files in your Airflow DAGs folder.
   - Ensure the Snowflake connection details are properly configured.
   - Trigger the ETL and ELT DAGs from the Airflow Web UI.

2. **BI Tool Setup:**
   - Configure your Superset with your Snowflake credentials.
   - Import the `session_summary` table from the `analytics` schema.
   - Create a WAU chart by aggregating the weekly active users and renaming the metric to "WAU".

