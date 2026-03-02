# =============================================================================
# FAAS4U — Window Normalization & Filtering
# =============================================================================
from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore

logger = logging.getLogger(__name__)

# Column mapping from French Silver schema to canonical names
_COLUMN_MAPPINGS: dict[str, str] = {
    "ventes_reelles_qty": "actual_qty",
    "algorithme": "algo_name",
}


def normalize_inputs(df: DataFrame) -> DataFrame:
    """
    Renames French columns to canonical names, creates date_month from periode,
    and validates required columns.
    """
    for old_col, new_col in _COLUMN_MAPPINGS.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Normalize date to first day of month
    if "periode" in df.columns:
        df = df.withColumn("date_month", F.trunc(F.col("periode"), "month"))

    required = ["sku_id", "algo_name", "date_month", "actual_qty", "forecast_qty"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.warning(f"Missing columns after normalization: {missing}")

    n_products = df.select("sku_id").distinct().count()
    n_months = df.select("date_month").distinct().count()
    logger.info(f"Normalized: {df.count()} rows, {n_products} products, {n_months} months.")
    return df


def get_max_month(df: DataFrame) -> datetime:
    """Returns the maximum date_month in the DataFrame."""
    row = df.agg(F.max("date_month").alias("max_date")).collect()[0]
    max_date = row["max_date"]
    logger.info(f"max_month = {max_date}")
    return max_date


def filter_window(df: DataFrame, max_month: datetime, window_months: int) -> DataFrame:
    """
    Returns rows where date_month is within the last `window_months` months
    ending at `max_month` (inclusive).
    """
    start = F.add_months(F.lit(max_month), -window_months + 1)
    df_w = df.filter((F.col("date_month") >= start) & (F.col("date_month") <= F.lit(max_month)))
    logger.info(f"Window {window_months}M: {df_w.count()} rows.")
    return df_w
