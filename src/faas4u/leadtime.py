# =============================================================================
# FAAS4U — Lead Time Impact Module
# Computes safety stock requirements and stockout risk based on forecast error
# volatility per product, per analytical window.
# Also computes Pearson correlation between lead time and forecast error metrics.
# =============================================================================
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore

from src.faas4u.config import (
    DEFAULT_LEAD_TIME_DAYS,
    LEADTIME_THRESHOLDS,
    VOLATILITY_METRIC_PREFERENCE,
)

logger = logging.getLogger(__name__)


def compute_leadtime_impact(
    df_perf: DataFrame,
    run_id: str,
    generated_at: datetime,
    date_max_window: datetime,
    df_master_leadtime: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Computes gld_leadtime_impact from the performance metrics table.

    For each (product_id, analytical_window_months), selects the BEST algorithm
    (lowest MAPE), extracts its volatility metric (WAPE preferred, RMSE fallback),
    and derives safety stock days and stockout risk severity.

    Parameters
    ----------
    df_perf : DataFrame
        The gld_performance_metrics DataFrame (must include product_id,
        algo_name, analytical_window_months, mape, wape, rmse).
    run_id : str
        Unique execution identifier.
    generated_at : datetime
        Execution timestamp.
    date_max_window : datetime
        Max month anchor for output.
    df_master_leadtime : DataFrame, optional
        Master data with (product_id, lead_time_days). If None, uses default.

    Returns
    -------
    DataFrame
        Conforms to gld_leadtime_impact schema.
    """
    # ── Step 1: Select best algo per (product_id, analytical_window_months) ──
    win_rank = Window.partitionBy("product_id", "analytical_window_months").orderBy(F.col("mape").asc())

    df_best = (
        df_perf
        .withColumn("_rank", F.row_number().over(win_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # ── Step 2: Determine volatility metric ──────────────────────────────────
    # WAPE in gld_performance_metrics is expressed in PERCENT (e.g. 25.0 = 25%).
    # We divide by 100 to obtain a 0–1 ratio used as the volatility coefficient.
    # Fallback: RMSE (absolute units — clamp to [0,1] below).
    has_wape = "wape" in [c.lower() for c in df_best.columns]
    has_rmse = "rmse" in [c.lower() for c in df_best.columns]

    if has_wape:
        df_vol = (
            df_best
            .withColumn("volatility_metric_name", F.lit("wape"))
            .withColumn(
                "volatility_metric_value",
                F.when(F.col("wape").isNotNull(), F.col("wape") / 100.0)  # % → [0,1] ratio
                 .otherwise(F.lit(0.0)),
            )
        )
    elif has_rmse:
        df_vol = (
            df_best
            .withColumn("volatility_metric_name", F.lit("rmse"))
            .withColumn(
                "volatility_metric_value",
                F.when(F.col("rmse").isNotNull(), F.col("rmse"))
                 .otherwise(F.lit(0.0)),
            )
        )
    else:
        logger.warning("Neither wape nor rmse found in perf metrics. Defaulting volatility to 0.")
        df_vol = (
            df_best
            .withColumn("volatility_metric_name", F.lit("none"))
            .withColumn("volatility_metric_value", F.lit(0.0))
        )

    # Clamp volatility between 0 and 1
    df_vol = df_vol.withColumn(
        "volatility_metric_value",
        F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.col("volatility_metric_value"))),
    )

    # ── Step 3: Join master lead time or use default ─────────────────────────
    if df_master_leadtime is not None:
        df_lt = (
            df_vol
            .join(
                df_master_leadtime.select(
                    F.col("product_id").alias("_lt_pid"),
                    F.col("lead_time_days").alias("_lt_days"),
                ),
                F.col("product_id") == F.col("_lt_pid"),
                "left",
            )
            .withColumn(
                "lead_time_days",
                F.coalesce(F.col("_lt_days"), F.lit(DEFAULT_LEAD_TIME_DAYS)),
            )
            .withColumn(
                "lead_time_days_source",
                F.when(F.col("_lt_days").isNotNull(), F.lit("masterdata"))
                 .otherwise(F.lit("default")),
            )
            .drop("_lt_pid", "_lt_days")
        )
    else:
        df_lt = (
            df_vol
            .withColumn("lead_time_days", F.lit(DEFAULT_LEAD_TIME_DAYS))
            .withColumn("lead_time_days_source", F.lit("default"))
        )

    # ── Step 4: Compute safety stock and risk ────────────────────────────────
    df_lt = df_lt.withColumn(
        "safety_stock_days_required",
        F.round(F.col("lead_time_days") * F.col("volatility_metric_value"), 2),
    )

    # ratio = safety_stock_days / lead_time_days  (== volatility after clamp)
    low_max = LEADTIME_THRESHOLDS["LOW_MAX"]
    med_max = LEADTIME_THRESHOLDS["MEDIUM_MAX"]
    high_max = LEADTIME_THRESHOLDS["HIGH_MAX"]

    df_lt = df_lt.withColumn(
        "risk_severity",
        F.when(F.col("volatility_metric_value") < low_max, F.lit("LOW"))
         .when(F.col("volatility_metric_value") < med_max, F.lit("MEDIUM"))
         .when(F.col("volatility_metric_value") < high_max, F.lit("HIGH"))
         .otherwise(F.lit("CRITICAL")),
    )

    df_lt = df_lt.withColumn(
        "stockout_risk_flag",
        F.col("risk_severity").isin("HIGH", "CRITICAL"),
    )

    # ── Step 5: Attach audit columns and select final schema ─────────────────
    df_out = (
        df_lt
        .withColumn("date_max_window", F.lit(date_max_window).cast("timestamp"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("generated_at", F.lit(generated_at).cast("timestamp"))
        .select(
            "product_id",
            "analytical_window_months",
            "lead_time_days",
            "lead_time_days_source",
            "volatility_metric_name",
            "volatility_metric_value",
            "safety_stock_days_required",
            "stockout_risk_flag",
            "risk_severity",
            "date_max_window",
            "run_id",
            "generated_at",
        )
    )

    logger.info(f"gld_leadtime_impact: {df_out.count()} rows computed.")
    return df_out


def compute_leadtime_correlation(
    df_leadtime: DataFrame,
    df_perf: DataFrame,
    window: int,
    run_id: str,
    generated_at: datetime,
    date_max_window: datetime,
) -> DataFrame:
    """
    Computes Pearson correlation between lead_time_days and forecast error
    metrics (WAPE, sMAPE) across products for a given analytical window.

    Produces gld_leadtime_correlation: one row per corr_metric_name per window.
    If fewer than 3 products are available, correlation_value is set to null.

    Parameters
    ----------
    df_leadtime : DataFrame
        The gld_leadtime_impact DataFrame for this window.
    df_perf : DataFrame
        The gld_performance_metrics DataFrame for this window.
    window : int
        The analytical window in months.
    run_id : str
        Unique execution identifier.
    generated_at : datetime
        Execution timestamp.
    date_max_window : datetime
        Max month anchor for output.

    Returns
    -------
    DataFrame
        Conforms to gld_leadtime_correlation schema.
    """
    from pyspark.sql import Row  # type: ignore

    # Get lead_time_days per product from leadtime impact
    df_lt_product = (
        df_leadtime
        .select("product_id", "lead_time_days")
        .dropDuplicates(["product_id"])
    )

    # Get best algo per product from performance metrics (lowest MAPE)
    win_rank = Window.partitionBy("product_id").orderBy(F.col("mape").asc())
    df_best_perf = (
        df_perf
        .withColumn("_rank", F.row_number().over(win_rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .select("product_id", "wape", "smape")
    )

    # Join lead time with error metrics
    df_joined = df_lt_product.join(df_best_perf, on="product_id", how="inner")

    n_products = df_joined.count()

    # Compute Pearson correlations
    corr_metrics = ["wape", "smape"]
    rows = []

    for metric_name in corr_metrics:
        if n_products >= 3 and metric_name in df_joined.columns:
            # Filter out nulls for the specific metric
            df_valid = df_joined.filter(
                F.col("lead_time_days").isNotNull() & F.col(metric_name).isNotNull()
            )
            n_valid = df_valid.count()

            if n_valid >= 3:
                try:
                    corr_val = df_valid.select(
                        F.corr("lead_time_days", metric_name).alias("corr")
                    ).collect()[0]["corr"]
                except Exception:
                    corr_val = None
            else:
                corr_val = None
        else:
            corr_val = None

        rows.append(Row(
            analytical_window_months=window,
            corr_metric_name=metric_name,
            correlation_value=float(corr_val) if corr_val is not None else None,
            n_products=n_products,
            date_max_window=date_max_window,
            run_id=run_id,
            generated_at=generated_at,
        ))

    # Get SparkSession from existing DataFrame
    spark = df_leadtime.sparkSession
    df_corr = spark.createDataFrame(rows)

    # Cast types explicitly for schema compliance
    df_corr = (
        df_corr
        .withColumn("analytical_window_months", F.col("analytical_window_months").cast("int"))
        .withColumn("correlation_value", F.col("correlation_value").cast("double"))
        .withColumn("n_products", F.col("n_products").cast("long"))
        .withColumn("date_max_window", F.col("date_max_window").cast("timestamp"))
        .withColumn("generated_at", F.col("generated_at").cast("timestamp"))
        .select(
            "analytical_window_months",
            "corr_metric_name",
            "correlation_value",
            "n_products",
            "date_max_window",
            "run_id",
            "generated_at",
        )
    )

    logger.info(f"gld_leadtime_correlation: {len(rows)} rows for window {window}M (n_products={n_products}).")
    return df_corr
