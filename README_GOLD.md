# Gold Layer – Analytics & Feature Engineering

## Purpose of the Gold Layer

The Gold layer represents the **analytical contract** of this pipeline. It transforms cleaned and conformed Silver-layer data into **explicit, feature-engineered datasets** designed for oil market analysis.

This layer is built with the assumption that its primary consumers are:

* Market analysts
* Data scientists / quants
* Business intelligence and dashboarding tools

The focus is on **clarity, correctness, and analytical intent**, with transformations implemented using **PySpark** to reflect scalable, production-style data processing.

---

## Why This Project (Commodities Focus)

Oil markets are inherently:

* Time-series driven
* Macro-sensitive
* Influenced by cross-asset relationships

This project focuses on oil (Brent & WTI) because it provides a realistic environment to:

* Engineer rolling statistical features
* Model spreads between benchmarks
* Analyse correlations across energy assets
* Incorporate financial and macroeconomic context

The Gold layer is designed to mirror how commodity data is typically prepared for **market analysis**, not just how it is stored.

---

## Gold Layer Design Principles

* **Explicit features**: All derived metrics are named, documented, and reproducible
* **No hidden logic**: Feature calculations are transparent and auditable
* **Analytics-first**: Tables are shaped for consumption, not further transformation
* **Layer isolation**: Gold logic depends only on Silver outputs
* **Spark-native execution**: Feature engineering is implemented in PySpark for scalability

---

## Inputs

The Gold layer consumes curated Silver tables, including:

* Cleaned daily energy prices
* Financial index time series
* Conformed macroeconomic indicators

All inputs are aligned to a common date dimension prior to feature engineering.

---

## Implemented Energy Market Features

The following features are fully implemented for oil markets:

### Daily Returns

* Daily percentage returns for energy assets

Returns form the foundation for downstream statistical analysis and cross-asset comparison.

### Rolling Statistics

* 7-day rolling averages
* 30-day rolling averages

These smooth short-term volatility and support trend analysis.

### Price Spreads

* Brent–WTI spread

Spreads are treated as first-class features due to their importance in oil market dynamics.

### Correlations

* Rolling correlations between energy assets

These features support cross-asset relationship analysis and regime identification.

---

## Financial & Macro Features (In Progress)

Financial and macroeconomic indicators are currently being engineered and prepared for integration into the Gold layer, including:

* Dollar Index (DXY)
* Equity market indices
* UK unemployment data

These features will provide broader economic context once joined with energy-market features.

---

## Target Gold Output

The target output of the Gold layer is a **wide, analytics-ready table** where:

* Each row represents a trading day
* Energy prices and derived features are primary
* Financial and macro features are aligned and additive

This structure is designed to support:

* Exploratory market analysis
* Dashboarding and visualisation
* Predictive modeling (future extension)

---

## Gold Data Flow

```
Silver Energy Tables      Silver Index Tables      Silver Macro Tables
          │                        │                        │
          └───────────┬────────────┴────────────┬───────────┘
                      ↓
           Feature Engineering (PySpark)
        (returns, spreads, rolling statistics)
                      ↓
           Gold Analytics Table
```

---

## MVP Status & Roadmap

The Gold layer is currently at **MVP stage**:

### Completed

* Energy-market feature engineering
* Daily returns, rolling statistics, spreads, correlations
* Spark-based feature transformations

### In Progress

* Financial and macro feature engineering
* Final Gold-layer joins

### Planned Extensions

* Dashboarding and visualisation layer
* Additional statistical and market features
* SQL-based access patterns (where appropriate)
* Orchestration and data quality checks

---

## Design Intent

This Gold layer is intentionally designed to demonstrate **how analytical datasets for commodity markets are engineered** in practice, with an emphasis on:

* Thoughtful feature selection
* Clear data contracts
* Scalable Spark-based transformations
* Honest, incremental development

It reflects how production data teams typically build analytics layers: iteratively, transparently, and with end users in mind.
