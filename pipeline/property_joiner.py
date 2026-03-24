"""
pipeline/property_joiner.py
---------------------------
Joins rental_property and property_reviews Iceberg tables,
then generates one complete JSON file per property.

Tables     : local.property_db.rental_property
             local.property_db.property_reviews
Join key   : rental_property.id  ==  property_reviews.gen_id
Output     : data/final_data/GEN-<id>.json
Engine     : Spark RDD  →  Row  →  dict  →  JSON file
Dedup      : dropDuplicates on (gen_id, review_id) before aggregation
Maintenance: expire_snapshots + remove_orphan_files on both tables

Output shape
------------
{
  "id":               "GEN-3005942",
  "feed_provider_id": "3005942",
  "property_name":    "Anvil Barn...",
  "property_slug":    "anvil-barn...",
  "country_code":     "GB",
  "currency":         "GBP",
  "usd_price":        0.0,
  "star_rating":      4.0,
  "review_score":     9.0,
  "commission":       null,
  "meal_plan":        null,
  "published":        true,
  "data_quality_flag":"GOOD",
  "total_reviews":    3,
  "reviews": [
    {
      "review_id": 5068520533, "review_date": "2024-08-16",
      "score": 10.0, "summary": "A lovely holiday", ...
    }
  ]
}
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
import os, sys, json

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from spark_utils import build_spark
from logger import log, flush_logs
from pipeline.transform import (
    joiner_deduplicate_reviews,
    joiner_aggregate_per_property,
    
)
from pipeline.json_generator import generate_json_files
from snapshot import expire_snapshots, remove_orphan_files

_OUTPUT_DIR = config.OUTPUT_FINAL_JSON
os.makedirs(_OUTPUT_DIR, exist_ok=True)


# -----------------------------------------------------------------------------
# 1.  Load both Iceberg tables
# -----------------------------------------------------------------------------
def load_tables(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df_rental  = spark.table(config.ICEBERG_PROPERTY_TABLE)
    df_reviews = spark.table(config.ICEBERG_REVIEWS_TABLE)
    log("READ", "Tables loaded",
        rental_rows=df_rental.count(), reviews_rows=df_reviews.count())
    return df_rental, df_reviews


# -----------------------------------------------------------------------------
# 2.  Join
#     rental_property  INNER JOIN  property_reviews
#     Rename shared columns on the reviews side to avoid ambiguity.
# -----------------------------------------------------------------------------
def join_tables(df_rental: DataFrame, df_reviews: DataFrame) -> DataFrame:
    shared = {
        "property_name", "property_slug", "country_code", "currency",
        "star_rating", "review_score", "published", "data_quality_flag",
    }
    df_rev = df_reviews
    for col in shared:
        df_rev = df_rev.withColumnRenamed(col, f"r_{col}")

    df_joined = df_rental.join(
        df_rev,
        df_rental["id"] == df_rev["gen_id"],
        how="inner",
    ).drop("gen_id", "source_id_r")

    log("JOIN", "Join complete", rows=df_joined.count())
    return df_joined


# -----------------------------------------------------------------------------
# 4.  Maintenance — both tables
# -----------------------------------------------------------------------------
def run_maintenance(spark: SparkSession) -> None:
    log("MAINT", "Running maintenance on both tables")
    db = config.ICEBERG_DATABASE

    for tbl in ("rental_property", "property_reviews"):
        table_ref = f"{db}.{tbl}"
        expire_snapshots(spark, table_ref)
        remove_orphan_files(spark, table_ref)

    log("MAINT", "Maintenance complete")


# -----------------------------------------------------------------------------
# 5.  Verification
# -----------------------------------------------------------------------------
def verify(written: list[str]) -> None:
    print("\n[PropertyJoiner] ── Sample output files ──")
    for path in written[:5]:
        print(f"  {path}")
    if len(written) > 5:
        print(f"  … and {len(written) - 5} more")
    print(f"\n[PropertyJoiner] Total: {len(written)} files → {_OUTPUT_DIR}")


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
def run() -> None:
    log("INIT", "PropertyJoiner starting")

    spark = build_spark("PropertyJoiner")

    df_rental, df_reviews   = load_tables(spark)
    df_reviews_clean        = joiner_deduplicate_reviews(df_reviews)
    df_joined               = join_tables(df_rental, df_reviews_clean)
    df_agg                  = joiner_aggregate_per_property(df_joined)
    written                 = generate_json_files(df_agg)

    run_maintenance(spark)
    verify(written)

    log("DONE", "PropertyJoiner complete", files_written=len(written))
    flush_logs()
    spark.stop()
