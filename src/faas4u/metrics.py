# =============================================================================
# FAAS4U — Metrics Module
# Compute row-level detail, aggregated KPIs, and leaderboard.
# =============================================================================
from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

logger = logging.getLogger(__name__)


def compute_detailed_results(
    df_w: DataFrame,
    window: int,
    run_id: str,
    generated_at: datetime,
    date_max_window: datetime,
) -> DataFrame:
    """Produces gld_algo_test_results_detail for one analytical window."""
    df = (
        df_w
        .withColumn("signed_error", F.col("actual_qty") - F.col("forecast_qty"))
        .withColumn("abs_error", F.abs(F.col("signed_error")))
        .withColumn("squared_error", F.pow(F.col("signed_error"), 2))
        .withColumn(
            "ape",
            F.when(F.col("actual_qty") != 0, F.col("abs_error") / F.col("actual_qty"))
             .otherwise(F.lit(None).cast("double")),
        )
        # sMAPE element: 2 * |actual - forecast| / (|actual| + |forecast|)
        # Returns null when both actual and forecast are 0 (division by zero)
        .withColumn(
            "smape_element",
            F.when(
                (F.abs(F.col("actual_qty")) + F.abs(F.col("forecast_qty"))) > 0,
                (2.0 * F.col("abs_error"))
                / (F.abs(F.col("actual_qty")) + F.abs(F.col("forecast_qty"))),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("is_zero_actual", F.col("actual_qty") == 0)
        .withColumn("analytical_window_months", F.lit(window).cast("int"))
        .withColumn("date_max_window", F.lit(date_max_window).cast("timestamp"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("generated_at", F.lit(generated_at).cast("timestamp"))
        .select(
            F.col("sku_id").alias("product_id"),
            F.col("algo_name"),
            F.col("date_month"),
            "analytical_window_months",
            "actual_qty",
            "forecast_qty",
            "signed_error",
            "abs_error",
            "squared_error",
            "ape",
            "smape_element",   # intermediate — consumed by compute_performance_metrics
            "is_zero_actual",
            "date_max_window",
            "run_id",
            "generated_at",
        )
    )
    return df


def compute_performance_metrics(
    df_detail: DataFrame,
    run_id: str,
    generated_at: datetime,
    date_max_window: datetime,
    window: int,
) -> DataFrame:
    """Produces gld_performance_metrics for one analytical window."""
    df = (
        df_detail
        .groupBy("product_id", "algo_name")
        .agg(
            (F.mean("ape") * 100).alias("mape"),
            (F.sum("abs_error") / F.sum("actual_qty") * 100).alias("wape"),
            F.sqrt(F.mean("squared_error")).alias("rmse"),
            F.mean("abs_error").alias("mae"),
            (F.mean("smape_element") * 100).alias("smape"),
            F.mean("signed_error").alias("bias"),
            F.sum("signed_error").alias("_sum_err"),
            F.mean("abs_error").alias("_mad"),
            F.count("*").alias("n_obs"),
        )
        .withColumn(
            "tracking_signal",
            F.when(F.col("_mad") > 0, F.col("_sum_err") / F.col("_mad"))
             .otherwise(F.lit(0.0)),
        )
        .withColumn("theils_u", F.lit(None).cast("double"))
        .withColumn("analytical_window_months", F.lit(window).cast("int"))
        .withColumn("date_max_window", F.lit(date_max_window).cast("timestamp"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("generated_at", F.lit(generated_at).cast("timestamp"))
        .drop("_sum_err", "_mad")
        .select(
            "product_id",
            "algo_name",
            "analytical_window_months",
            "mape",
            "wape",
            "rmse",
            "mae",
            "smape",
            "bias",
            "tracking_signal",
            "theils_u",
            "n_obs",
            "date_max_window",
            "run_id",
            "generated_at",
        )
    )
    return df


def compute_leaderboard(
    df_perf: DataFrame,
    segment_col: str = "segment_abcxyz",
) -> DataFrame:
    """
    Produces gld_algo_leaderboard.
    If segment_col is missing it defaults to 'ALL'.
    """
    if segment_col not in df_perf.columns:
        df_perf = df_perf.withColumn(segment_col, F.lit("ALL"))

    df = (
        df_perf
        .groupBy("analytical_window_months", segment_col, "algo_name",
                 "date_max_window", "run_id", "generated_at")
        .agg(
            F.mean("mape").alias("avg_mape"),
            F.mean("wape").alias("avg_wape"),
            F.mean("rmse").alias("avg_rmse"),
            F.mean("bias").alias("avg_bias"),
            F.countDistinct("product_id").alias("count_products"),
        )
    )

    win_rank = Window.partitionBy("analytical_window_months", segment_col).orderBy(F.asc("avg_mape"))
    df = df.withColumn("rank_mape", F.dense_rank().over(win_rank))

    # Rename to canonical schema name
    if segment_col != "segment_abcxyz":
        df = df.withColumnRenamed(segment_col, "segment_abcxyz")

    return df.select(
        "analytical_window_months",
        "segment_abcxyz",
        "algo_name",
        "avg_mape",
        "avg_wape",
        "avg_rmse",
        "avg_bias",
        "count_products",
        "rank_mape",
        "date_max_window",
        "run_id",
        "generated_at",
    )
