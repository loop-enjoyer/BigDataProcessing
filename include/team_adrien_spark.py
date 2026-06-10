"""PySpark Gold KPI job for Lab 4 team_adrien."""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from include.paths import curated_kpis, raw_parquet, reference_targets, report_json


TX_SCHEMA = T.StructType(
    [
        T.StructField("tx_id", T.StringType(), False),
        T.StructField("category", T.StringType(), True),
        T.StructField("payment_method", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("amount_eur", T.DoubleType(), True),
        T.StructField("ts", T.LongType(), True),
    ]
)

TARGET_SCHEMA = T.StructType(
    [
        T.StructField("category", T.StringType(), False),
        T.StructField("target_revenue_eur", T.DoubleType(), True),
    ]
)


def _spark() -> SparkSession:
    return (
        SparkSession.builder.appName("team_adrien_daily_kpis")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Read Silver, enforce types, remove invalid rows and deduplicate tx_id."""
    source = raw_parquet(logical_date)
    if not source.exists():
        raise FileNotFoundError(f"Silver parquet not found: {source}")

    return (
        spark.read.schema(TX_SCHEMA)
        .parquet(str(source))
        .withColumn("amount_eur", F.col("amount_eur").cast("double"))
        .withColumn("event_ts", F.to_timestamp("ts"))
        .filter(F.col("tx_id").isNotNull())
        .filter(F.col("category").isNotNull())
        .filter(F.col("country").isNotNull())
        .filter(F.col("amount_eur") > 0)
        .dropDuplicates(["tx_id"])
    )


def transform_2(
    spark: SparkSession,
    df: DataFrame,
    logical_date: str,
    *,
    with_reference: bool = False,
) -> DataFrame:
    """Enrich transactions with date, basket segment and optional category targets."""
    enriched = (
        df.withColumn("logical_date", F.lit(logical_date))
        .withColumn("event_date", F.to_timestamp(F.col("ts") / 1_000_000))
        .withColumn(
            "basket_segment",
            F.when(F.col("amount_eur") >= 150, F.lit("high"))
            .when(F.col("amount_eur") >= 50, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
    )

    if with_reference and reference_targets().exists():
        targets = spark.read.schema(TARGET_SCHEMA).option("header", True).csv(str(reference_targets()))
        return enriched.join(F.broadcast(targets), on="category", how="left")

    return enriched.withColumn("target_revenue_eur", F.lit(None).cast("double"))


def transform_3(df: DataFrame) -> DataFrame:
    """Aggregate daily revenue KPIs by category and country."""
    return (
        df.groupBy("logical_date", "event_date", "category", "country")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.sum("amount_eur"), 2).alias("revenue_eur"),
            F.round(F.avg("amount_eur"), 2).alias("avg_basket_eur"),
            F.round(F.max("amount_eur"), 2).alias("max_basket_eur"),
            F.countDistinct("payment_method").alias("payment_methods"),
            F.first("target_revenue_eur", ignorenulls=True).alias("target_revenue_eur"),
        )
        .withColumn(
            "target_completion_pct",
            F.when(
                F.col("target_revenue_eur").isNotNull() & (F.col("target_revenue_eur") > 0),
                F.round(F.col("revenue_eur") / F.col("target_revenue_eur") * 100, 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .orderBy(F.desc("revenue_eur"), "category", "country")
    )


def _write_json_atomic(payload, path):
    def default_serializer(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=default_serializer), encoding="utf-8")
    tmp.replace(path)


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Run Spark transformations and write idempotent Gold Parquet + dashboard JSON."""
    spark = _spark()
    try:
        cleaned = transform_1(spark, logical_date)
        enriched = transform_2(spark, cleaned, logical_date, with_reference=with_reference)
        kpis = transform_3(enriched)

        row_count = cleaned.count()
        if row_count == 0:
            raise RuntimeError(f"No valid positive transactions for {logical_date}")

        total_revenue = float(enriched.agg(F.round(F.sum("amount_eur"), 2)).first()[0])
        gold_path = curated_kpis(logical_date)
        (
            kpis.coalesce(1)
            .write.mode("overwrite")
            .parquet(str(gold_path))
        )

        top_rows = [
            row.asDict(recursive=True)
            for row in kpis.limit(10).collect()
        ]
        category_rows = [
            row.asDict(recursive=True)
            for row in enriched.groupBy("category")
            .agg(
                F.count("*").alias("tx_count"),
                F.round(F.sum("amount_eur"), 2).alias("revenue_eur"),
            )
            .orderBy(F.desc("revenue_eur"))
            .collect()
        ]

        payload = {
            "logical_date": logical_date,
            "status": "ok",
            "spark_version": spark.version,
            "silver_valid_rows": int(row_count),
            "total_revenue_eur": total_revenue,
            "gold_path": str(gold_path),
            "top_category_country": top_rows,
            "revenue_by_category": category_rows,
        }
        _write_json_atomic(payload, report_json(logical_date))
        return payload
    finally:
        spark.stop()


def run_daily_cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run team_adrien daily Spark KPIs")
    parser.add_argument("--date", required=True, help="Logical date YYYY-MM-DD")
    parser.add_argument("--with-reference", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_daily(args.date, with_reference=args.with_reference), indent=2))


if __name__ == "__main__":
    run_daily_cli()
