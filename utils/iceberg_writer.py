"""
utils/iceberg_writer.py
-----------------------
Iceberg table writer for the PySpark property pipeline.
 
Handles final_output DataFrame only:
  1. Schema preparation  (StructType definition)
  2. Single-file write   (coalesce to 1 Parquet file)
  3. Partition-wise write (Iceberg, partitioned by country_code)
"""
 
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
)
 
import config
from logger import log
 
 
# ---------------------------------------------------------------------------
# 1. Schema
# ---------------------------------------------------------------------------
 
RENTAL_PROPERTY_SCHEMA = StructType([
    StructField("id",                StringType(),  False),  # GEN-<source_id>
    StructField("feed_provider_id",  StringType(),  True),
    StructField("property_name",     StringType(),  True),
    StructField("property_slug",     StringType(),  True),
    StructField("country_code",      StringType(),  True),   # partition key
    StructField("currency",          StringType(),  True),
    StructField("usd_price",         DoubleType(),  True),
    StructField("star_rating",       DoubleType(),  True),
    StructField("review_score",      DoubleType(),  True),
    StructField("commission",        DoubleType(),  True),
    StructField("meal_plan",         StringType(),  True),
    StructField("published",         BooleanType(), True),
    StructField("data_quality_flag", StringType(),  True),
])