"""
Lab 4 - Team Adrien capstone DAG.

Pipeline:
  Bronze CSV -> Silver Parquet (DuckDB) -> Gold KPIs (PySpark) -> JSON dashboard
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import curated_kpis, report_json
from include.team_adrien_spark import run_daily


DEFAULT_ARGS = {
    "owner": "team_adrien",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="team_adrien",
    description="Retail KPI medallion pipeline: Bronze -> Silver -> Gold -> Serve",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone", "team_adrien"],
) as dag:

    wait_for_vendor_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )

    @task
    def ingest_bronze_to_silver(ds: str) -> dict:
        """Convert the vendor CSV for ds into a typed Silver Parquet file."""
        return ingest_day(ds)

    @task
    def validate_silver_quality(ds: str) -> dict:
        """Fail loudly for empty/corrupt data, including vendor_drop --corrupt."""
        return validate_silver(ds, min_rows=10, min_revenue=0.01)

    @task
    def build_gold_kpis(ds: str) -> dict:
        """Run the PySpark transformations and write Gold + Serve outputs."""
        return run_daily(ds)

    @task
    def publish_dashboard(ds: str) -> dict:
        """Check the JSON dashboard is available for downstream consumers."""
        path = report_json(ds)
        if not path.exists():
            raise FileNotFoundError(f"Dashboard report not found: {path}")
        return {"logical_date": ds, "report_path": str(path), "status": "ready"}

    @task
    def summarize_outputs(ds: str) -> dict:
        """Small terminal task that makes the produced artifacts visible in logs/XCom."""
        gold_path = curated_kpis(ds)
        dashboard_path = report_json(ds)
        if not gold_path.exists():
            raise FileNotFoundError(f"Gold parquet folder not found: {gold_path}")
        if not dashboard_path.exists():
            raise FileNotFoundError(f"Dashboard report not found: {dashboard_path}")
        return {
            "logical_date": ds,
            "gold_path": str(gold_path),
            "dashboard_path": str(dashboard_path),
        }

    silver = ingest_bronze_to_silver()
    checked = validate_silver_quality()
    gold = build_gold_kpis()
    published = publish_dashboard()
    summary = summarize_outputs()

    wait_for_vendor_csv >> silver >> checked >> gold >> published >> summary
