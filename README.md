# Oil Market Analytics Pipeline

A production-style data engineering project focused on building analytics-ready datasets for global oil markets. The project follows a medallion architecture (Bronze → Silver → Gold) and is designed as a professional portfolio project, prioritising data modeling, clarity, and analytical usefulness over tooling complexity.

This repository demonstrates how commodity market data can be ingested, cleaned, and transformed into well-defined analytical outputs suitable for downstream analysis, dashboards, and modeling.

---

## Project Objectives

* Design and implement a layered data pipeline aligned with industry best practices
* Apply strong data modeling and transformation discipline
* Engineer analytics-ready datasets for commodity market analysis
* Demonstrate senior-level data engineering decision-making and trade-offs

---

## Architecture Overview

```
External Data Sources
        ↓
   Bronze Layer
   (raw ingestion)
        ↓
   Silver Layer
 (cleaned & conformed)
        ↓
   Gold Layer
 (analytics & features)
```

Each layer has a clear responsibility and contract, allowing the pipeline to be re-run, extended, or refactored without impacting downstream consumers.

---

## Data Domains

### Energy Markets

* Brent Crude Oil
* WTI Crude Oil
* Natural Gas

### Financial Context

* Dollar Index (DXY)
* S&P 500
* FTSE 100

### Macroeconomic

* UK & US Unemployment
* UK & US Inflation
* UK & US Interest Rate
* UK & US GDP

The scope is intentionally limited to high-signal datasets commonly used in real-world commodity analysis.

---

## Technology Stack

* Python
* Pandas
* PySpark
* Kafka
* Databricks Notebooks
* Git / GitHub

The stack is intentionally chosen to reflect modern data engineering patterns while keeping the focus on core engineering fundamentals.

---

## Repository Structure

```
├── README.md
├── README_GOLD.md
├── .github/
├── airflow/
├── config/
│   ├── client.properties
│   └── config.yaml
├── data/
│   ├── uk_gdp/
│   └── uk_interest_rate/
├── notebooks/
│   ├── 01_Bronze_Ingestion/
│   ├── 02_Silver_Transformation/
│   └── 03_Gold_Analytics/
├── src/
│   ├── ingestion/
│   │   ├── historical_oil_prices.py
│   │   ├── index_ingest.py
│   │   ├── kafka_consumer.py
│   │   ├── load_config.py
│   │   ├── macro_api_ingest.py
│   │   └── oil_api_ingest.py
│   ├── transform/
│   │   ├── bronze/
│   │   │   ├── bronze_energy_prices.py
│   │   │   ├── bronze_index.py
│   │   │   ├── bronze_macro.py
│   │   │   └── bronze_macro_local.py
│   │   ├── silver/
│   │   │   ├── __init__.py
│   │   │   ├── silver_energy_prices.py
│   │   │   ├── silver_index.py
│   │   │   └── silver_macro.py
│   │   └── gold/
│   │       └── (in progress)
├── tests/
├── .gitignore
└── requirements.txt
```

---

## How to Run

1. Clone the repository
2. Create a Python environment
3. Run notebooks sequentially by layer:

   * 01_Bronze_Ingestion
   * 02_Silver_Transformation
   * 03_Gold_Analytics

Each layer is designed to be idempotent and can be re-run independently.

---

## Project Status & Roadmap

This project is under active development.

Current focus:

* Gold-layer macro feature engineering
* Final analytics table assembly

Planned extensions:

* Airflow-based orchestration
* Data quality checks
* Dashboarding and visualisation
* Additional statistical and market features

---

## Author

George Baldwin
Senior Data Engineer

This project forms part of an ongoing professional portfolio focused on building realistic, production-quality data engineering systems.
