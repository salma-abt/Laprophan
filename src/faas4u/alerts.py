# =============================================================================
# FAAS4U — Drift Alerts Module
# =============================================================================
from __future__ import annotations

import logging

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore

from src.faas4u.config import THRESHOLDS

logger = logging.getLogger(__name__)


def compute_alerts(df_perf: DataFrame) -> DataFrame:
    """
    Produces gld_alertes_derive: rows where tracking signal or MAPE exceeds thresholds.

    recommendation_text and model_source are initialized to None here;
    they are populated by the narratives module in the orchestrator.
    """
    ts_thresh = THRESHOLDS["tracking_signal_abs_threshold"]
    mape_thresh = THRESHOLDS["mape_red_threshold"]

    df = (
        df_perf
        .withColumn(
            "alert_flag",
            (F.abs(F.col("tracking_signal")) >= ts_thresh) | (F.col("mape") >= mape_thresh),
        )
        .filter(F.col("alert_flag") == True)
        .withColumn(
            "severity",
            F.when(F.abs(F.col("tracking_signal")) >= ts_thresh * 1.5, "CRITICAL")
             .when(
                (F.abs(F.col("tracking_signal")) >= ts_thresh) | (F.col("mape") >= mape_thresh),
                "HIGH",
            )
             .when(F.col("mape") >= mape_thresh * 0.5, "MEDIUM")
             .otherwise("LOW"),
        )
        .withColumn("recommendation_text", F.lit(None).cast("string"))
        .withColumn("model_source", F.lit(None).cast("string"))
        .select(
            "product_id",
            "algo_name",
            "analytical_window_months",
            "tracking_signal",
            "alert_flag",
            "severity",
            "recommendation_text",
            "model_source",
            "date_max_window",
            "run_id",
            "generated_at",
        )
    )
    logger.info(f"Alerts generated: {df.count()} rows.")
    return df
