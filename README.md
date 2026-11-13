# 📊 League of Legends Data Lakehouse | Medallion Architecture

*(Project Currently in Active Development)*

An academic and non-commercial Big Data Engineering portfolio project designed to ingest, process, and analyze League of Legends E-Sports data (LAS Challenger Tier) at scale using **Databricks**, **PySpark**, and **Delta Lake**.

## Project Overview

This project transitions from traditional transactional ETL pipelines to a distributed Data Lakehouse environment. It utilizes the **Medallion Architecture** to process large volumes of match history data from the top 200 Challenger players, ultimately calculating macro-level champion meta and player performance metrics.

## Medallion Architecture Pipeline

The data flows through three distinct layers within the Databricks File System (DBFS), utilizing the Delta format for ACID compliance and time travel capabilities:

* ** Bronze Layer (Raw Data):** 
  * Ingestion of highly nested, raw JSON responses from the Riot Games API (`Match-V5`, `League-V4`).
  * Features adaptive rate-limiting logic to respect API constraints during mass extraction.
  * Data is stored immutably to preserve a historical source of truth.

* ** Silver Layer (Curated Data):**
  * Distributed processing using **PySpark**.
  * Complex nested arrays (e.g., match participants, telemetry) are exploded and flattened (`get_json_object`, `from_json`).
  * Schema enforcement, null handling, and column derivation (e.g., KDA calculations) are applied here.

* ** Gold Layer (Business Aggregations):**
  * Final, analyst-ready tables optimized for Business Intelligence and dashboarding.
  * Uses PySpark aggregations (`groupBy`, `agg`) to generate Data Marts such as:
    * `gold_champion_meta`: Global win rates, pick rates, and average damage per champion in the current patch.
    * `gold_player_stats`: Challenger leaderboard tracking KDA and vision scores.

## Technology Stack

* **Compute & Orchestration:** Databricks (Community Edition)
* **Distributed Processing:** Apache Spark (PySpark)
* **Storage Format:** Delta Lake
* **Language:** Python 3.11+
* **Data Source:** Riot Games Developer API

## Status

This repository is currently being set up. The codebase is being migrated to Databricks notebooks. Full deployment is pending the approval of a Personal API Key from the Riot Developer Portal to enable the ingestion of the complete Challenger ladder dataset.
