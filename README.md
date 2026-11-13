# 📊 League of Legends Data Lakehouse | Medallion Architecture

An academic and non-commercial Big Data Engineering portfolio project designed to ingest, process, and analyze League of Legends E-Sports data (LAS Challenger Tier) at scale using **Databricks**, **PySpark**, and **Delta Lake**.

## Project Overview

This project transitions from traditional transactional ETL pipelines to a distributed Data Lakehouse environment. It utilizes the **Medallion Architecture** to process large volumes of match history data from the top 200 Challenger players, ultimately calculating macro-level champion meta and player performance metrics.

Developed as part of my Computer Science Engineering (Data Engineering specialization) studies at Duoc UC.

## Medallion Architecture Pipeline

The data flows through three distinct layers within the Databricks environment, utilizing the Delta format for ACID compliance and time travel capabilities:

### Bronze Layer (Raw Data)
* **Ingestion:** Extracts highly nested, raw JSON responses from the Riot Games API (`Match-V5`, `League-V4`).
* **Resilience & Optimization:** Features adaptive rate-limiting logic (HTTP 429 handling) and in-memory deduplication (using Python `set()`) to respect API constraints during mass extraction (10,000+ matches).
* **Storage:** Data is stored immutably with an ingestion timestamp to preserve a historical source of truth.
* **Security:** API credentials are dynamically fetched using `dbutils.secrets` to adhere to Cloud Security best practices.

### Silver Layer (Curated Data)
* **Processing:** Distributed processing using **PySpark**.
* **Transformation:** Complex nested arrays (e.g., match participants) are parsed and flattened using `get_json_object`, `from_json`, and `explode`.
* **Data Quality:** Schema enforcement, null handling, and safe column derivation (e.g., KDA calculations handling division by zero).

### Gold Layer (Business Aggregations)
Final, analyst-ready tables optimized for Business Intelligence:
* **`gold_champion_meta`**: Global win rates, pick rates, and average damage per champion in the current patch.
* **`gold_player_stats`**: Challenger leaderboard tracking KDA and win rates. *Note: Applies a statistical significance filter (`total_matches >= 20`) to eliminate data skew from low-frequency players.*

## Technology Stack

* **Compute & Orchestration:** Databricks
* **Distributed Processing:** Apache Spark (PySpark)
* **Storage Format:** Delta Lake
* **Language:** Python 3
* **Data Source:** Riot Games Developer API
* **Cloud Security:** Databricks Secrets

## How to Run

1. Import the `lol-databricks-lakehouse.py` notebook into your Databricks environment.
2. Configure a Databricks Secret scope named `riot_api` with your personal key stored under `api_key`.
3. Attach the notebook to a cluster (Community Edition compatible).
4. Run all cells sequentially to build the Bronze, Silver, and Gold Delta tables.