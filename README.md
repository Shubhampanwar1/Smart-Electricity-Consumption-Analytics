# Smart Electricity Consumption Analytics (Australia AEMO Grid)

An end-to-end Big Data pipeline and analytics project built with **Apache Spark (PySpark)**, **Databricks**, **Delta Lake**, and **Power BI**. This project processes 20 years of Australian National Electricity Market (AEMO) demand data, enriches it with weather and regional tariff context, forecasts daily peak demand using Machine Learning, and visualizes the results in an interactive executive dashboard.

---

## 1. Problem Statement

Electricity grids experience heavy demand fluctuations driven by time of day, extreme temperatures, and seasonal shifts. Analyzing two decades of multi-regional, high-frequency grid readings is slow and impractical with traditional single-machine tools like Excel or standard SQL scripts. 

Grid operators and energy planners need a scalable way to:
* Process millions of historical load records reliably.
* Understand how weather and weekday/weekend behavior drive peak usage.
* Forecast daily peak demand accurately to avoid grid overload and manage peak tariffs.

## 2. Project Objectives

* Build a scalable **Medallion Architecture** (Bronze, Silver, Gold) using Apache Spark and Delta Lake on Databricks.
* Combine multi-source datasets (historical grid demand, weather metrics, and state-level demographics/tariffs) into a unified analytical model.
* Engineer domain-specific features (Southern Hemisphere seasons, Peak/Off-Peak windows, per-capita load, and estimated hourly grid value).
* Train a time-series regression model to predict daily peak electricity load.
* Connect Databricks directly to **Power BI Desktop** for live, interactive reporting.

---

## 3. Dataset Description & Sources

1. **Electricity Load Data (Primary Source):** 20 years of regional electricity load CSV files from the Australian Energy Market Operator (AEMO) covering major regions (New South Wales, Queensland, Victoria, South Australia, Tasmania, etc.). Stored in Databricks Unity Catalog Volumes (`/Volumes/workspace/default/australiya_raw_data`).
2. **Historical Weather Data:** Daily mean temperature, maximum temperature, and precipitation data fetched per region via the **Open-Meteo Historical Archive API** (with an automated seasonal climate profile fallback when running in restricted network environments).
3. **Regional Demographics & Tariffs:** State-level population, average/peak electricity tariffs (AUD/kWh), and industrial share percentages used to calculate per-capita load and estimated grid costs.

---

## 4. Technologies & Tools Used

| Category | Tools / Technologies |
| :--- | :--- |
| **Big Data Processing** | Apache Spark, PySpark, Spark SQL |
| **Lakehouse Platform** | Databricks (Free Edition), Unity Catalog Volumes, Delta Lake |
| **Machine Learning** | Python, Pandas, NumPy, Scikit-Learn (`HistGradientBoostingRegressor`) |
| **BI & Visualization** | Power BI Desktop (connected via Databricks SQL Warehouse) |
| **Version Control** | Git & GitHub |

---

## 5. Project Architecture & Workflow

We followed the **Medallion Architecture** to keep raw ingestion, data cleaning, and business reporting cleanly separated:

```text
[Raw CSVs + Weather API + Tariffs]
               │
               ▼
┌─────────────────────────────────┐
│  BRONZE LAYER (Raw Delta)       │  Parallel CSV read + API/lookup ingestion
│  • bronze_electricity_load      │
│  • bronze_weather               │
│  • bronze_demographics_tariffs  │
└─────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  SILVER LAYER (Cleaned/Joined)  │  Unpivot wide columns, drop nulls/dupes,
│  • silver_smart_electricity     │  engineer seasons & peak hours, join tables
└─────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  GOLD LAYER (BI + ML Tables)    │  Pre-aggregated KPIs & ML forecast outputs
│  • gold_regional_monthly_summary│
│  • gold_hourly_demand_profile   │
│  • gold_daily_peak_demand       │
│  • gold_ml_peak_predictions     │
└─────────────────────────────────┘
               │
               ▼
      [Power BI Dashboard]           Connected via Databricks SQL Warehouse
```

### Step-by-Step Pipeline Implementation
* **Bronze Layer:** Used PySpark to read all yearly `Australia_AEMO*.csv` files recursively in parallel and stored them as ACID-compliant managed Delta tables in Unity Catalog.
* **Silver Layer:** Unpivoted the wide regional columns into a normalized `(Timestamp, Region, Load_MW)` format using Spark SQL's `stack()` expression. Filtered out invalid/zero readings, removed duplicates, added Australian seasonal labels and Peak (4 PM–9 PM) buckets, and joined weather and tariff tables.
* **Gold Layer:** Created pre-aggregated Delta tables optimized for fast BI queries (monthly trends, 24-hour weekday vs. weekend load profiles, and daily peak summaries).
* **ML Peak Forecasting:** Engineered 1-day lag and 7-day rolling average features per region. Used a chronological train/test split (holding out the most recent 2 years for testing) so no future data leaked into training, and trained a `HistGradientBoostingRegressor` to predict daily peak demand.

---

## 6. Apache Spark Concepts & Functionalities Applied

* **Distributed Multi-File Ingestion:** Used `spark.read` with `recursiveFileLookup`, `pathGlobFilter`, and `inferSchema` to ingest 20 years of CSVs in a single pass.
* **DataFrame Transformations & Spark SQL:** Applied `stack()` for wide-to-long unpivoting, conditional expressions (`F.when().otherwise()`), date/time extraction (`F.year`, `F.hour`, `F.dayofweek`), and multi-key left joins.
* **Aggregations:** Leveraged distributed `groupBy().agg()` operations to compute regional peaks, averages, and peak-to-average ratios.
* **Delta Lake & Unity Catalog:** Stored every pipeline stage as managed Delta tables (`format("delta")`, `overwriteSchema`) inside `workspace.default`, enabling reliable schema evolution and direct querying via the Serverless SQL Warehouse.

---

## 7. Final Output & Key Results

Our Power BI dashboard connects directly to the four Gold Delta tables and includes a global `State_Name` slicer linked via a `Dim_State` dimension table so all visuals cross-filter smoothly.

**Key Findings from the Dashboard:**
* **All-Time Peak Load:** `14.58K MW` across the monitored grid.
* **Average Daily Load:** `4.39K MW`.
* **ML Forecast Accuracy:** Our Gradient Boosting model achieved a Mean Absolute Error (MAE) of **`217.32 MW`** on unseen test years, closely tracking real daily peak fluctuations.
* **Behavioral Patterns:** Weekday electricity usage shows a sharp evening ramp-up between **4:00 PM and 9:00 PM**, while extreme high temperatures strongly correlate with summer peak spikes.

<img width="1418" height="795" alt="image" src="https://github.com/user-attachments/assets/853cac92-b2ad-48b2-bb39-3bf3fda105db" />

---

## 8. How to Run This Project

1. Upload the AEMO yearly CSV files into a Databricks Unity Catalog Volume (e.g., `/Volumes/workspace/default/australiya_raw_data`).
2. Import the notebook (`Smart_Electricity_Analytics.ipynb`) into Databricks and attach it to Serverless Compute.
3. Run **Cells 1 through 6** sequentially to generate all Bronze, Silver, and Gold Delta tables (plus optional CSV backups in `powerbi_exports/`).
4. Open **Power BI Desktop**, select **Get Data -> Databricks**, enter your SQL Warehouse Server Hostname, HTTP Path, and Personal Access Token (PAT), and load the four `gold_*` tables.

---

## 9. Conclusion

This project demonstrates how Apache Spark and Databricks can turn raw, multi-decade energy grid data into actionable insights. By combining distributed ETL, Medallion data modeling, machine learning forecasting, and live Power BI reporting, the pipeline provides both historical grid visibility and reliable peak-demand predictions.

---

## Team Members

* **Shubham**
* **Siddiq Khan** 
* **Aaditya Saini**
