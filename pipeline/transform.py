"""
transform.py
------------
Shared DataFrame transformation functions for pipeline steps.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)

import config
from logger import log


# -----------------------------------------------------------------------------
# Rental writer transformations
# -----------------------------------------------------------------------------

def rental_extract_details(df: DataFrame) -> DataFrame:
    """Flatten property.json → source_id, property_name, country_code,
    currency, star_rating, review_score."""
    log("EXTRACT", "Extracting details fields")
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered = (
        [f for f in config.NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in config.NAME_PREFERENCE]
    )
    name_cols = [F.col(f"`name`.`{field}`") for field in ordered]
    best_name = F.coalesce(*name_cols) if len(name_cols) > 1 else name_cols[0]

    return df.select(
        F.col("id").cast(StringType()).alias("source_id"),
        best_name.alias("property_name"),
        F.trim(F.upper(F.col("location.country"))).alias("country_code"),
        F.col("currency"),
        F.col("rating.stars").cast(DoubleType()).alias("star_rating"),
        F.col("rating.review_score").cast(DoubleType()).alias("review_score"),
    )


def rental_extract_search(df: DataFrame) -> DataFrame:
    """Flatten search.json → search_id, usd_price, commission_pct,
    meal_plan, deep_link_url, search_currency."""
    log("EXTRACT", "Extracting search fields")
    return df.select(
        F.col("id").cast(StringType()).alias("search_id"),
        F.col("price.book").cast(DoubleType()).alias("usd_price"),
        F.col("commission.percentage").cast(DoubleType()).alias("commission_pct"),
        F.col("products").getItem(0)
         .getField("policies").getField("meal_plan").getField("meals")
         .cast(StringType()).alias("meal_plan"),
        F.col("deep_link_url"),
        F.col("currency").alias("search_currency"),
    )


def rental_clean(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = df.filter(F.col("source_id").isNotNull() & (F.col("source_id") != ""))
    dropped = before - cleaned.count()
    log("CLEAN", "Dropped null source_id rows", dropped=dropped)
    return cleaned


def rental_deduplicate(df: DataFrame, key: str) -> DataFrame:
    before = df.count()
    df_dedup = df.dropDuplicates([key])
    log("DEDUP", f"Deduplicated on '{key}'", removed=before - df_dedup.count())
    return df_dedup


def rental_join_details_search(
    details_df: DataFrame, search_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Inner join + left-anti join on source_id == search_id."""
    log("JOIN", "Joining details × search")
    matched = details_df.join(
        search_df, details_df["source_id"] == search_df["search_id"], how="inner"
    )
    unmatched = details_df.join(
        search_df, details_df["source_id"] == search_df["search_id"], how="left_anti"
    )
    log("JOIN", "Join complete", matched=matched.count(), unmatched=unmatched.count())
    return matched, unmatched


def _rental_make_slug(col: F.Column) -> F.Column:
    return F.lower(F.regexp_replace(F.trim(col), r"[^a-zA-Z0-9]+", "-"))


def rental_build_final_output(matched_df: DataFrame) -> tuple[DataFrame, int]:
    """Produce the 13-column standardized DataFrame + data_quality_flag."""
    log("TRANSFORM", "Building final output")
    defaulted = matched_df.filter(F.col("usd_price").isNull()).count()

    df = matched_df.select(
        F.concat_ws("-", F.lit("GEN"), F.col("source_id")).alias("id"),
        F.col("source_id").alias("feed_provider_id"),
        F.col("property_name"),
        _rental_make_slug(F.col("property_name")).alias("property_slug"),
        F.col("country_code"),
        F.coalesce(F.col("currency"), F.lit(config.DEFAULT_CURRENCY)).alias("currency"),
        F.coalesce(F.col("usd_price"), F.lit(config.DEFAULT_USD_PRICE))
         .cast(DoubleType()).alias("usd_price"),
        F.coalesce(F.col("star_rating"), F.lit(config.DEFAULT_STAR_RATING))
         .cast(DoubleType()).alias("star_rating"),
        F.coalesce(F.col("review_score"), F.lit(config.DEFAULT_REVIEW_SCORE))
         .cast(DoubleType()).alias("review_score"),
        F.col("commission_pct").alias("commission"),
        F.col("meal_plan"),
        F.lit(config.DEFAULT_PUBLISHED).cast(BooleanType()).alias("published"),
    ).withColumn(
        "data_quality_flag",
        F.when(
            F.col("property_name").isNull()
            | F.col("usd_price").isNull()
            | (F.length(F.col("country_code")) != 2),
            F.lit("NEEDS_REVIEW"),
        ).otherwise(F.lit("GOOD")),
    )

    log(
        "TRANSFORM",
        "Final output ready",
        rows=df.count(),
        columns=len(df.columns),
        defaulted_prices=defaulted,
    )
    return df, defaulted


# -----------------------------------------------------------------------------
# Reviews writer transformations
# -----------------------------------------------------------------------------

def reviews_flatten_properties(df: DataFrame) -> DataFrame:
    """Filter nulls, resolve localised name, extract core fields."""
    log("EXTRACT", "Flattening properties")
    df_clean = df.filter(F.col("id").isNotNull())
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered = (
        [f for f in config.NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in config.NAME_PREFERENCE]
    )
    name_cols = [F.col(f"`name`.`{field}`") for field in ordered]
    best_name = F.coalesce(*name_cols) if len(name_cols) > 1 else name_cols[0]

    return df_clean.select(
        F.col("id").cast("long").alias("source_id"),
        best_name.alias("raw_name"),
        F.col("location.city").alias("raw_city"),
        F.col("currency").alias("raw_currency"),
        F.col("location.country").alias("raw_country"),
        F.col("rating.stars").cast("double").alias("star_rating_raw"),
        F.col("rating.review_score").cast("double").alias("review_score_raw"),
    ).withColumn(
        "property_name",
        F.when(
            F.col("raw_name").isNotNull() & (F.trim(F.col("raw_name")) != ""),
            F.trim(F.col("raw_name")),
        ).otherwise(F.trim(F.col("raw_city"))),
    )


def reviews_flatten_reviews(df: DataFrame) -> DataFrame:
    """Explode the reviews array — one row per review."""
    log("EXTRACT", "Exploding reviews")
    return (
        df
        .withColumn("review", F.explode_outer(F.col("reviews")))
        .select(
            F.col("id").cast("long").alias("source_id_r"),
            F.col("review.id").cast("long").alias("review_id"),
            F.col("review.date").alias("review_date"),
            F.col("review.score").cast("double").alias("review_individual_score"),
            F.col("review.language").alias("review_language"),
            F.col("review.summary").alias("review_summary"),
            F.col("review.positive").alias("review_positive"),
            F.col("review.negative").alias("review_negative"),
            F.col("review.reviewer.name").alias("reviewer_name"),
            F.col("review.reviewer.country").alias("reviewer_country"),
            F.col("review.reviewer.travel_purpose").alias("reviewer_travel_purpose"),
            F.col("review.reviewer.type").alias("reviewer_type"),
        )
    )


def _reviews_make_slug(col: F.Column) -> F.Column:
    return (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.trim(F.lower(col)), r"[^\w\s-]", ""),
                    r"[\s_]+", "-"),
                r"-{2,}", "-"),
            r"^-|-$", "")
    )


def reviews_join_and_enrich(df_prop: DataFrame, df_rev: DataFrame) -> DataFrame:
    """Left-join then add gen_id, slug, country_code, quality flag."""
    log("JOIN", "Joining properties × reviews")
    df_joined = df_prop.join(
        df_rev,
        df_prop["source_id"] == df_rev["source_id_r"],
        how="left",
    )
    log("JOIN", "Join complete", rows=df_joined.count())

    return (
        df_joined
        .withColumn("gen_id", F.concat(F.lit("GEN-"), F.col("source_id").cast("string")))
        .withColumn("feed_provider_id", F.col("source_id").cast("string"))
        .withColumn("property_slug", _reviews_make_slug(F.col("property_name")))
        .withColumn("country_code", F.upper(F.col("raw_country")))
        .withColumn(
            "currency",
            F.when(
                F.col("raw_currency").isNull() | (F.trim(F.col("raw_currency")) == ""),
                F.lit(config.DEFAULT_CURRENCY),
            ).otherwise(F.col("raw_currency")),
        )
        .withColumn("star_rating", F.coalesce(F.col("star_rating_raw"), F.lit(config.DEFAULT_STAR_RATING)))
        .withColumn("review_score", F.coalesce(F.col("review_score_raw"), F.lit(config.DEFAULT_REVIEW_SCORE)))
        .withColumn("published", F.lit(True))
        .withColumn(
            "data_quality_flag",
            F.when(
                F.col("property_name").isNull()
                | (F.trim(F.col("property_name")) == "")
                | F.col("country_code").isNull()
                | (F.col("review_score") == 0.0)
                | (F.col("star_rating") == 0.0),
                F.lit("NEEDS_REVIEW"),
            ).otherwise(F.lit("GOOD")),
        )
    )


def reviews_prepare_for_iceberg(df: DataFrame) -> DataFrame:
    """Select final columns, add review_year partition column."""
    return (
        df
        .withColumn(
            "review_year",
            F.when(
                F.col("review_date").isNotNull(),
                F.year(F.to_date(F.col("review_date"), "yyyy-MM-dd")),
            ).otherwise(F.lit(None).cast("int")),
        )
        .select(
            F.col("source_id").cast(LongType()).alias("property_id"),
            F.col("gen_id"),
            F.col("property_name"),
            F.col("property_slug"),
            F.col("country_code"),
            F.col("currency"),
            F.col("star_rating").cast(DoubleType()),
            F.col("review_score").cast(DoubleType()),
            F.col("published").cast(BooleanType()),
            F.col("data_quality_flag"),
            F.col("review_id").cast(LongType()),
            F.col("review_date"),
            F.col("review_year").cast(IntegerType()),
            F.col("review_individual_score").cast(DoubleType()),
            F.col("review_language"),
            F.col("review_summary"),
            F.col("review_positive"),
            F.col("review_negative"),
            F.col("reviewer_name"),
            F.col("reviewer_country"),
            F.col("reviewer_travel_purpose"),
            F.col("reviewer_type"),
        )
    )


# -----------------------------------------------------------------------------
# Json generator transformations
# -----------------------------------------------------------------------------

def json_deduplicate(df: DataFrame) -> DataFrame:
    before = df.count()
    df_dedup = df.dropDuplicates(["gen_id", "review_id"])
    dropped = before - df_dedup.count()
    if dropped:
        log("DEDUP", "Removed duplicate review rows", dropped=dropped)
    else:
        log("DEDUP", "No duplicates found", rows=df_dedup.count())
    return df_dedup


def json_aggregate_per_property(df: DataFrame) -> DataFrame:
    df_struct = df.withColumn(
        "review_struct",
        F.when(
            F.col("review_id").isNotNull(),
            F.struct(
                F.col("review_id"), F.col("review_date"), F.col("review_year"),
                F.col("review_individual_score"), F.col("review_language"),
                F.col("review_summary"), F.col("review_positive"),
                F.col("review_negative"), F.col("reviewer_name"),
                F.col("reviewer_country"), F.col("reviewer_travel_purpose"),
                F.col("reviewer_type"),
            ),
        ).otherwise(F.lit(None)),
    )

    df_agg = (
        df_struct
        .groupBy(
            "property_id", "gen_id", "property_name", "property_slug",
            "country_code", "currency", "star_rating", "review_score",
            "published", "data_quality_flag",
        )
        .agg(
            F.collect_list(
                F.when(F.col("review_struct").isNotNull(), F.col("review_struct"))
            ).alias("reviews")
        )
        .orderBy("gen_id")
    )

    log("AGG", "Aggregated per property", distinct_properties=df_agg.count())
    return df_agg


# -----------------------------------------------------------------------------
# Property joiner transformations
# -----------------------------------------------------------------------------

def joiner_deduplicate_reviews(df: DataFrame) -> DataFrame:
    before = df.count()
    df_dedup = df.dropDuplicates(["gen_id", "review_id"])
    dropped = before - df_dedup.count()
    if dropped:
        log("DEDUP", "Removed duplicate review rows", dropped=dropped)
    else:
        log("DEDUP", "No duplicates found", rows=df_dedup.count())
    return df_dedup


def joiner_aggregate_per_property(df: DataFrame) -> DataFrame:
    df_struct = df.withColumn(
        "review_struct",
        F.when(
            F.col("review_id").isNotNull(),
            F.struct(
                F.col("review_id"), F.col("review_date"), F.col("review_year"),
                F.col("review_individual_score"), F.col("review_language"),
                F.col("review_summary"), F.col("review_positive"),
                F.col("review_negative"), F.col("reviewer_name"),
                F.col("reviewer_country"), F.col("reviewer_travel_purpose"),
                F.col("reviewer_type"),
            ),
        ).otherwise(F.lit(None)),
    )

    df_agg = (
        df_struct
        .groupBy(
            "id", "feed_provider_id", "property_name", "property_slug",
            "country_code", "currency", "usd_price", "star_rating",
            "review_score", "commission", "meal_plan", "published",
            "data_quality_flag",
        )
        .agg(
            F.collect_list(
                F.when(F.col("review_struct").isNotNull(), F.col("review_struct"))
            ).alias("reviews")
        )
        .orderBy("id")
    )

    log("AGG", "Aggregated per property", distinct_properties=df_agg.count())
    return df_agg
