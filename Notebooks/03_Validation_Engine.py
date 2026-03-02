# =============================================================================
# FAAS4U — Laprophan | Couche Gold | Validation Engine (Orchestrator)
# Fichier  : 03_Validation_Engine.py
# Auteure  : Salma | PFE Mundiapolis 2026
# Rôle     : Orchestre les 9 tables Gold du moteur multi-horizon.
#            Toute la logique métier est déléguée aux modules src/faas4u/*.
# =============================================================================
from __future__ import annotations

import logging
import os
import sys
import uuid
from datetime import datetime

# Ensure project root is on PYTHONPATH
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.faas4u.config import (
    ANALYTICAL_WINDOWS,
    ANALYTICAL_WINDOWS_MODE,
    OUTPUT_DIR,
    OUTPUT_MODE,
    TABLE_ALERTES,
    TABLE_DASHBOARD_SPECS,
    TABLE_DETAIL,
    TABLE_FVA,
    TABLE_LEADTIME,
    TABLE_LEADTIME_CORR,
    TABLE_LEADERBOARD,
    TABLE_PERF_METRICS,
    TABLE_REPORTS,
    TABLE_SILVER,
    enforce_schema,
)
from src.faas4u.alerts import compute_alerts
from src.faas4u.fva import compute_fva
from src.faas4u.io import write_output
from src.faas4u.leadtime import compute_leadtime_impact, compute_leadtime_correlation
from src.faas4u.metrics import compute_detailed_results, compute_leaderboard, compute_performance_metrics
from src.faas4u.narratives import safe_claude_alert_recommendation, safe_claude_dashboard_spec, safe_claude_report
from src.faas4u.summaries import build_summary_dict
from src.faas4u.windows import filter_window, get_max_month, normalize_inputs

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run_pipeline(spark_session=None) -> None:
    """
    Main orchestration: reads Silver, computes all Gold tables across
    analytical windows, and writes outputs.

    Window mode is controlled by ANALYTICAL_WINDOWS_MODE env var:
      - "EXEC" (default): executive windows [1, 3, 6, 12]
      - "FULL": backtesting across all 1..24 months
    """
    logger.info("╔══ FAAS4U Validation Engine — Starting ══╗")
    logger.info(f"Windows mode = {ANALYTICAL_WINDOWS_MODE} → {ANALYTICAL_WINDOWS}")

    # ── Run metadata (generated ONCE) ────────────────────────────────────────
    run_id = str(uuid.uuid4())
    generated_at = datetime.now()
    logger.info(f"run_id       = {run_id}")
    logger.info(f"generated_at = {generated_at}")

    # ── Spark session ────────────────────────────────────────────────────────
    if spark_session is None:
        try:
            from pyspark.sql import SparkSession  # type: ignore
            builder = SparkSession.builder
            
            # Local Windows stabilization: force 127.0.0.1 to avoid SocketException
            if os.name == "nt":
                builder = (
                    builder.config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.python.worker.reuse", "false")                # Avoid connection resets
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false") # Safer on 3.13
                )

            spark_session = builder.getOrCreate()
        except ImportError:
            logger.critical("PySpark required. Install pyspark or run inside Fabric.")
            raise

    # ── Load Silver ──────────────────────────────────────────────────────────
    try:
        df_silver = spark_session.read.format("delta").load(TABLE_SILVER)
    except Exception:
        # Local fallback: try CSV
        csv_path = os.path.join(OUTPUT_DIR, "forecasts_clean.csv")
        if os.path.exists(csv_path):
            df_silver = spark_session.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            logger.info(f"Loaded Silver from CSV fallback: {csv_path}")
        else:
            logger.critical("Cannot load Silver table (Delta or CSV).")
            raise

    # ── Normalize ────────────────────────────────────────────────────────────
    df_norm = normalize_inputs(df_silver)
    max_month = get_max_month(df_norm)
    date_max_window = max_month
    logger.info(f"date_max_window = {date_max_window}")

    # ── Accumulators ─────────────────────────────────────────────────────────
    from pyspark.sql import DataFrame  # type: ignore
    acc: dict[str, DataFrame | None] = {
        TABLE_DETAIL: None,
        TABLE_PERF_METRICS: None,
        TABLE_FVA: None,
        TABLE_ALERTES: None,
        TABLE_LEADERBOARD: None,
        TABLE_LEADTIME: None,
        TABLE_LEADTIME_CORR: None,
    }

    def _union(key: str, df: DataFrame) -> None:
        acc[key] = df if acc[key] is None else acc[key].unionByName(df)

    # ── Multi-Horizon Loop ───────────────────────────────────────────────────
    for w in ANALYTICAL_WINDOWS:
        logger.info(f"── Window {w}M ──")
        df_w = filter_window(df_norm, max_month, w)
        if df_w.rdd.isEmpty():
            logger.warning(f"  No data for window {w}M. Skipping.")
            continue

        # Detail (includes intermediate smape_element for perf aggregation)
        dt = compute_detailed_results(df_w, w, run_id, generated_at, date_max_window)

        # Strip smape_element before accumulating detail (not in enforced schema)
        dt_clean = dt.drop("smape_element")
        _union(TABLE_DETAIL, dt_clean)

        # Performance (consumes dt WITH smape_element for sMAPE aggregation)
        perf = compute_performance_metrics(dt, run_id, generated_at, date_max_window, w)
        _union(TABLE_PERF_METRICS, perf)

        # FVA
        fva = compute_fva(perf)
        _union(TABLE_FVA, fva)

        # Alerts (recommendation_text populated later)
        alerts = compute_alerts(perf)
        _union(TABLE_ALERTES, alerts)

        # Leaderboard
        lb = compute_leaderboard(perf)
        _union(TABLE_LEADERBOARD, lb)

        # Lead Time Impact
        lt = compute_leadtime_impact(perf, run_id, generated_at, date_max_window)
        _union(TABLE_LEADTIME, lt)

        # Lead Time Correlation (new: error vs lead time Pearson)
        lt_corr = compute_leadtime_correlation(lt, perf, w, run_id, generated_at, date_max_window)
        _union(TABLE_LEADTIME_CORR, lt_corr)

    # ── Validate we have data ────────────────────────────────────────────────
    if acc[TABLE_DETAIL] is None:
        logger.critical("No data processed for any window. Aborting.")
        return

    # ── Populate Claude recommendations on alerts ────────────────────────────
    if acc[TABLE_ALERTES] is not None and not acc[TABLE_ALERTES].rdd.isEmpty():
        pdf_alerts = acc[TABLE_ALERTES].toPandas()
        recs, srcs = [], []
        for _, row in pdf_alerts.iterrows():
            txt, src = safe_claude_alert_recommendation(row.to_dict())
            recs.append(txt)
            srcs.append(src)
        pdf_alerts["recommendation_text"] = recs
        pdf_alerts["model_source"] = srcs
        acc[TABLE_ALERTES] = spark_session.createDataFrame(pdf_alerts)

    # ── Build summary for Claude narratives ──────────────────────────────────
    summary = build_summary_dict(
        acc[TABLE_PERF_METRICS], acc[TABLE_FVA], acc[TABLE_ALERTES], acc[TABLE_LEADERBOARD],
    )

    # ── Dashboard Specs ──────────────────────────────────────────────────────
    dash_json, dash_src = safe_claude_dashboard_spec(summary)
    from pyspark.sql import Row  # type: ignore
    df_dash = spark_session.createDataFrame([
        Row(
            spec_json=dash_json,
            scope="global",
            model_source=dash_src,
            run_id=run_id,
            generated_at=generated_at,
        )
    ])

    # ── Analytical Reports ───────────────────────────────────────────────────
    rep1, src1 = safe_claude_report(summary, "monthly_1page")
    rep2, src2 = safe_claude_report(summary, "monthly_detailed")
    df_reports = spark_session.createDataFrame([
        Row(report_type="monthly_1page", report_markdown=rep1, scope="global",
            analytical_window_months=None, model_source=src1, run_id=run_id, generated_at=generated_at),
        Row(report_type="monthly_detailed", report_markdown=rep2, scope="global",
            analytical_window_months=None, model_source=src2, run_id=run_id, generated_at=generated_at),
    ])

    # ── Write all 9 Gold tables (with schema enforcement) ────────────────────
    outputs = {
        TABLE_DETAIL: acc[TABLE_DETAIL],
        TABLE_PERF_METRICS: acc[TABLE_PERF_METRICS],
        TABLE_FVA: acc[TABLE_FVA],
        TABLE_ALERTES: acc[TABLE_ALERTES],
        TABLE_LEADERBOARD: acc[TABLE_LEADERBOARD],
        TABLE_LEADTIME: acc[TABLE_LEADTIME],
        TABLE_LEADTIME_CORR: acc[TABLE_LEADTIME_CORR],
        TABLE_DASHBOARD_SPECS: df_dash,
        TABLE_REPORTS: df_reports,
    }

    for table_name, df in outputs.items():
        if df is not None:
            df_enforced = enforce_schema(df, table_name)
            write_output(df_enforced, table_name, OUTPUT_MODE, OUTPUT_DIR)
        else:
            logger.warning(f"Skipping {table_name}: no data.")

    logger.info(f"╚══ FAAS4U Pipeline complete. run_id={run_id} ══╝")


if __name__ == "__main__":
    run_pipeline()
