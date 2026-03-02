# =============================================================================
# FAAS4U — Configuration Module
# Centralizes all environment variables, thresholds, paths, and strict schemas.
# =============================================================================
from __future__ import annotations

import os
import logging
from typing import Final

logger = logging.getLogger(__name__)

# ── Feature Flags ────────────────────────────────────────────────────────────
ENABLE_CLAUDE: bool = os.environ.get("ENABLE_CLAUDE", "false").lower() == "true"
ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")

# ── I/O ──────────────────────────────────────────────────────────────────────
OUTPUT_MODE: str = os.environ.get("OUTPUT_MODE", "auto")  # "auto" | "fabric" | "local"
OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "./outputs")

# ── Analytical Windows ───────────────────────────────────────────────────────
# EXEC mode: executive summary windows (default, backward compatible)
# FULL mode: backtesting across all 1..24 months (set env ANALYTICAL_WINDOWS_MODE=FULL)
ANALYTICAL_WINDOWS_EXEC: Final[list[int]] = [1, 3, 6, 12]
ANALYTICAL_WINDOWS_FULL: Final[list[int]] = list(range(1, 25))
ANALYTICAL_WINDOWS_MODE: str = os.environ.get("ANALYTICAL_WINDOWS_MODE", "EXEC").upper()
ANALYTICAL_WINDOWS: list[int] = (
    ANALYTICAL_WINDOWS_FULL if ANALYTICAL_WINDOWS_MODE == "FULL"
    else ANALYTICAL_WINDOWS_EXEC
)

# ── Business Thresholds ──────────────────────────────────────────────────────
THRESHOLDS: Final[dict] = {
    "tracking_signal_abs_threshold": 6.0,
    "mape_cibles": {"A": 10.0, "B": 20.0, "C": 40.0},
    "mape_red_threshold": 30.0,    # percent
    "wape_red_threshold": 25.0,    # percent
    "bias_abs_threshold": 10.0,    # percent
}

# ── Lead Time Configuration ──────────────────────────────────────────────────
DEFAULT_LEAD_TIME_DAYS: Final[int] = 45
VOLATILITY_METRIC_PREFERENCE: Final[list[str]] = ["wape", "rmse"]

LEADTIME_THRESHOLDS: Final[dict[str, float]] = {
    "LOW_MAX":      0.15,
    "MEDIUM_MAX":   0.25,
    "HIGH_MAX":     0.35,
    # CRITICAL: ratio >= HIGH_MAX
}

# ── Data Paths ───────────────────────────────────────────────────────────────
SILVER_PATH: Final[str] = "Tables/silver/"
GOLD_PATH:   Final[str] = "Tables/gold/"

TABLE_SILVER:          Final[str] = f"{SILVER_PATH}forecasts_clean"
TABLE_DETAIL:          Final[str] = f"{GOLD_PATH}gld_algo_test_results_detail"
TABLE_PERF_METRICS:    Final[str] = f"{GOLD_PATH}gld_performance_metrics"
TABLE_FVA:             Final[str] = f"{GOLD_PATH}gld_fva_results"
TABLE_ALERTES:         Final[str] = f"{GOLD_PATH}gld_alertes_derive"
TABLE_LEADERBOARD:     Final[str] = f"{GOLD_PATH}gld_algo_leaderboard"
TABLE_LEADTIME:        Final[str] = f"{GOLD_PATH}gld_leadtime_impact"
TABLE_LEADTIME_CORR:   Final[str] = f"{GOLD_PATH}gld_leadtime_correlation"
TABLE_DASHBOARD_SPECS: Final[str] = f"{GOLD_PATH}gld_dashboard_specs"
TABLE_REPORTS:         Final[str] = f"{GOLD_PATH}gld_analytical_reports"

# ── Strict Expected Schemas (ordered column lists) ───────────────────────────
EXPECTED_SCHEMAS: Final[dict[str, list[str]]] = {
    TABLE_DETAIL: [
        "product_id",
        "algo_name",
        "date_month",
        "analytical_window_months",
        "actual_qty",
        "forecast_qty",
        "signed_error",
        "abs_error",
        "squared_error",
        "ape",
        "is_zero_actual",
        "date_max_window",
        "run_id",
        "generated_at",
    ],
    TABLE_PERF_METRICS: [
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
    ],
    TABLE_FVA: [
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
    ],
    TABLE_ALERTES: [
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
    ],
    TABLE_LEADERBOARD: [
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
    ],
    TABLE_LEADTIME: [
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
    ],
    TABLE_LEADTIME_CORR: [
        "analytical_window_months",
        "corr_metric_name",
        "correlation_value",
        "n_products",
        "date_max_window",
        "run_id",
        "generated_at",
    ],
    TABLE_DASHBOARD_SPECS: [
        "spec_json",
        "scope",
        "model_source",
        "run_id",
        "generated_at",
    ],
    TABLE_REPORTS: [
        "report_type",
        "report_markdown",
        "scope",
        "analytical_window_months",
        "model_source",
        "run_id",
        "generated_at",
    ],
}


def enforce_schema(df, table_name: str):
    """
    Validates that a DataFrame has exactly the expected columns,
    then returns it with columns in the canonical order.
    Raises ValueError if columns mismatch.
    """
    expected = EXPECTED_SCHEMAS.get(table_name)
    if expected is None:
        raise ValueError(f"No expected schema defined for table: {table_name}")

    # Handle both Spark and Pandas DataFrames
    try:
        actual_cols = set(df.columns)
    except AttributeError:
        raise TypeError(f"Expected DataFrame, got {type(df)}")

    expected_set = set(expected)
    missing = expected_set - actual_cols
    extra   = actual_cols - expected_set

    if missing or extra:
        msg = f"Schema mismatch for {table_name}."
        if missing:
            msg += f" Missing: {missing}."
        if extra:
            msg += f" Extra: {extra}."
        raise ValueError(msg)

    # Reorder columns to canonical order
    try:
        # Spark DataFrame
        return df.select(*expected)
    except (TypeError, AttributeError):
        # Pandas DataFrame
        return df[expected]
