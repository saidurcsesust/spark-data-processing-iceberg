"""
spark_utils.py
--------------
Shared SparkSession builder for pipeline steps.
"""

from __future__ import annotations

import json
import os
import shutil
from typing import Mapping

from pyspark.sql import SparkSession

import config
from logger import log


def build_spark(app_name: str, extra_conf: Mapping[str, str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", config.ICEBERG_JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}.warehouse",
            config.ICEBERG_WAREHOUSE,
        )
        .config("spark.sql.iceberg.write.format.default", "parquet")
        # Use Kryo for faster JVM-side serialization.
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Enable Arrow optimization for pandas UDFs when available.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[{app_name}] SparkSession ready")
    return spark


def _split_table_name(table_name: str) -> tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.db.table, got: {table_name}")
    return parts[0], parts[1], parts[2]


def _expected_table_path(table_name: str) -> str:
    _, database, table = _split_table_name(table_name)
    return os.path.join(config.ICEBERG_WAREHOUSE, database, table)


def _metadata_json_path(table_dir: str) -> str | None:
    version_hint = os.path.join(table_dir, "metadata", "version-hint.text")
    if not os.path.exists(version_hint):
        return None

    with open(version_hint, encoding="utf-8") as fh:
        version = fh.read().strip()

    if not version:
        return None

    metadata_json = os.path.join(table_dir, "metadata", f"v{version}.metadata.json")
    return metadata_json if os.path.exists(metadata_json) else None


def _repair_reasons(table_name: str) -> list[str]:
    table_dir = _expected_table_path(table_name)
    metadata_json = _metadata_json_path(table_dir)
    if metadata_json is None:
        return []

    with open(metadata_json, encoding="utf-8") as fh:
        metadata = json.load(fh)

    reasons: list[str] = []
    expected_location = os.path.normpath(table_dir)
    actual_location = os.path.normpath(metadata.get("location", ""))
    if actual_location and actual_location != expected_location:
        reasons.append(
            f"metadata location points to {actual_location}, expected {expected_location}"
        )

    current_snapshot_id = metadata.get("current-snapshot-id")
    if current_snapshot_id is None:
        return reasons

    current_snapshot = next(
        (snap for snap in metadata.get("snapshots", []) if snap.get("snapshot-id") == current_snapshot_id),
        None,
    )
    if current_snapshot is None:
        reasons.append(f"current snapshot {current_snapshot_id} is missing from metadata")
        return reasons

    manifest_list = current_snapshot.get("manifest-list")
    if manifest_list and not os.path.exists(manifest_list):
        reasons.append(f"manifest list is missing: {manifest_list}")

    return reasons


def repair_local_table_if_needed(spark: SparkSession, table_name: str) -> None:
    reasons = _repair_reasons(table_name)
    if not reasons:
        return

    table_dir = _expected_table_path(table_name)
    log("REPAIR", "Resetting stale local Iceberg table", table=table_name, reasons=" | ".join(reasons))

    if spark.catalog.tableExists(table_name):
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    shutil.rmtree(table_dir, ignore_errors=True)
