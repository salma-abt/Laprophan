# =============================================================================
# FAAS4U — Forecast Value Added (FVA) Module
# =============================================================================
from __future__ import annotations

import logging

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

logger = logging.getLogger(__name__)


def compute_fva(df_perf: DataFrame) -> DataFrame:
    """
    Computes gld_fva_results per (product_id, analytical_window_months).

    FVA = algo_error_metric − planner_error_metric
    (positive FVA = planner improved accuracy)

    Uses WAPE as the error metric for consistency.
    """
    # Naive errors
    df_naive = (
        df_perf
        .filter(F.lower(F.col("algo_name")).isin("naive"))
        .select("product_id", "analytical_window_months",
                F.col("wape").alias("naive_error_metric"))
    )

    # Planner errors
    df_planner = (
        df_perf
        .filter(F.lower(F.col("algo_name")).isin("planner", "expert_dp"))
        .select("product_id", "analytical_window_months",
                F.col("wape").alias("planner_error_metric"))
    )

    # Best algo (excluding naive/planner)
    df_algos = df_perf.filter(
        ~F.lower(F.col("algo_name")).isin("naive", "planner", "expert_dp")
    )

    if df_algos.rdd.isEmpty():
        logger.warning("No algorithm data found for FVA. Returning empty.")
        return df_perf.limit(0).select(
            "product_id", "analytical_window_months",
            F.lit(None).cast("double").alias("naive_error_metric"),
            F.lit(None).cast("double").alias("algo_error_metric"),
            F.lit(None).cast("double").alias("planner_error_metric"),
            F.lit(None).cast("double").alias("fva_value"),
            F.lit("Unknown").alias("fva_flag"),
            F.lit(0).cast("long").alias("n_obs"),
            "date_max_window", "run_id", "generated_at",
        )

    win = Window.partitionBy("product_id", "analytical_window_months").orderBy(F.asc("wape"))
    df_best = (
        df_algos
        .withColumn("_r", F.row_number().over(win))
        .filter(F.col("_r") == 1)
        .select("product_id", "analytical_window_months",
                F.col("wape").alias("algo_error_metric"),
                "n_obs", "date_max_window", "run_id", "generated_at")
        .drop("_r")
    )

    # Join
    df_fva = (
        df_best
        .join(df_naive, ["product_id", "analytical_window_months"], "left")
        .join(df_planner, ["product_id", "analytical_window_months"], "left")
        .withColumn(
            "fva_value",
            F.round(F.col("algo_error_metric") - F.col("planner_error_metric"), 4),
        )
        .withColumn(
            "fva_flag",
            F.when(F.col("fva_value") > 0, "Positive")
             .when(F.col("fva_value") == 0, "Neutral")
             .when(F.col("fva_value") < 0, "Negative")
             .otherwise("Unknown"),
        )
        .select(
            "product_id",
            "analytical_window_months",
            "naive_error_metric",
            "algo_error_metric",
            "planner_error_metric",
            "fva_value",
            "fva_flag",
            "n_obs",
            "date_max_window",
            "run_id",
            "generated_at",
        )
    )
    return df_fva
