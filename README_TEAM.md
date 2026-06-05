# Team: Adrien

**DAG id:** `team_adrien`  
**Spark module:** `include/team_adrien_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

The pipeline produces a daily retail dashboard for business users who need to follow revenue by category and country. If the pipeline fails, the dashboard is not published and the team can see the failed Airflow task immediately.

---

## 2. Architecture

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/transactions_<ds>.csv` | `vendor_drop.py` |
| Silver | `data/raw/dt=<ds>/transactions.parquet` | DuckDB `ingest_day` |
| Gold | `data/curated/dt=<ds>/kpis_by_category_country.parquet` | PySpark `team_adrien_spark.py` |
| Serve | `data/reports/dashboard_<ds>.json` | JSON written by PySpark |

### Airflow tasks

| task_id | Role |
|---------|------|
| `wait_for_vendor_csv` | Waits for the vendor CSV in Bronze. |
| `ingest_bronze_to_silver` | Converts CSV to Silver Parquet using DuckDB. |
| `validate_silver_quality` | Checks row count and positive revenue; fails on corrupt input. |
| `build_gold_kpis` | Runs the PySpark transformations and writes Gold + dashboard JSON. |
| `publish_dashboard` | Verifies the dashboard JSON exists. |
| `summarize_outputs` | Exposes final artifact paths in logs/XCom. |

**Dependency graph:**

```text
wait_for_vendor_csv -> ingest_bronze_to_silver -> validate_silver_quality -> build_gold_kpis -> publish_dashboard -> summarize_outputs
```

---

## 3. Spark transformations

File: `include/team_adrien_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Reads Silver Parquet with an explicit schema, casts amounts/timestamps, filters invalid rows, deduplicates `tx_id`. |
| 2 | `transform_2` | Adds `logical_date`, `event_date`, `basket_segment`, and joins category revenue targets from `data/reference/category_targets.csv`. |
| 3 | `transform_3` | Aggregates KPIs by `logical_date`, `event_date`, `category`, `country`: transaction count, revenue, average basket, max basket, payment method diversity and target completion. |

---

## 4. Idempotence

The pipeline is idempotent for the same `ds`:

- Silver: `ingest_day` removes and rewrites `data/raw/dt=<ds>/transactions.parquet`.
- Gold: Spark writes `data/curated/dt=<ds>/kpis_by_category_country.parquet` with `mode("overwrite")`.
- Serve: `dashboard_<ds>.json` is rewritten through a temporary file and atomic replace.

Re-running the same logical date updates the same partition paths and does not append duplicates.

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler airflow dags unpause team_adrien
docker compose exec airflow-scheduler airflow dags backfill team_adrien -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
docker compose exec airflow-scheduler airflow dags trigger team_adrien -e 2026-06-03T00:00:00+00:00
```

Expected result: `validate_silver_quality` fails because `amount_sum=0.0`. In the Airflow UI, the task is red and the logs show `Validation failed: amount_sum=0.0 (corrupt day?)`.

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | Yes | Idempotent writes, retries, terminal artifact checks. |
| S Spark depth | Yes | Explicit schema, derived columns, broadcast reference join, aggregations. |
| O Orchestration | Yes | Six Airflow tasks with sensor, validation, Spark and publish stages. |
| Q Data quality | Yes | Silver validation catches empty or zero-revenue corrupt days. |
| P Custom | Yes | Dashboard includes top category/country KPIs and revenue by category. |
| X SparkSubmit | Partial | The Spark module has a CLI entrypoint; Airflow uses TaskFlow import. |

---

## 8. Demo script & backup

```bash
python scripts/vendor_drop.py --seed-pack --volume small
python scripts/vendor_drop.py --reference
docker compose up -d
docker compose exec airflow-scheduler airflow dags unpause team_adrien
docker compose exec airflow-scheduler airflow dags trigger team_adrien -e 2026-06-01T00:00:00+00:00
```

Check:

```bash
type data\reports\dashboard_2026-06-01.json
```

Backup before the demo:

```bash
copy data\incoming\transactions_2026-06-03.csv demo_backup\transactions_2026-06-03.csv
```

---

## 9. Production next steps

- Add schema drift alerts before ingestion.
- Store dashboards in an object store instead of a local volume.
- Add unit tests for transformations with small Spark DataFrames.
- Add SLA or notification callbacks for late vendor files and failed validations.
