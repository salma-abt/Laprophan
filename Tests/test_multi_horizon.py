# =============================================================================
# FAAS4U — Multi-Horizon Logic Tests
# =============================================================================
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession  # type: ignore

        s = SparkSession.builder.master("local[1]") \
            .appName("FAAS4U_HorizonTest") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .getOrCreate()
        s.sparkContext.setLogLevel("ERROR")
        yield s
        s.stop()
    except Exception as e:
        pytest.skip(f"PySpark not available: {e}")


@pytest.fixture
def sample_silver(spark):
    np.random.seed(99)
    months = pd.date_range("2023-01-01", periods=15, freq="MS").tolist()
    rows = []
    for sku in ["X1", "X2"]:
        for algo in ["Prophet", "ARIMAX"]:
            for m in months:
                rows.append({
                    "sku_id": sku,
                    "algorithme": algo,
                    "periode": m,
                    "ventes_reelles_qty": float(np.random.randint(50, 200)),
                    "forecast_qty": float(np.random.randint(40, 210)),
                    "segment_abcxyz": "AX",
                    "horizon_mois": 1,
                })
    return spark.createDataFrame(pd.DataFrame(rows))


def test_window_filtering(spark, sample_silver):
    from src.faas4u.windows import normalize_inputs, get_max_month, filter_window

    df = normalize_inputs(sample_silver)
    mx = get_max_month(df)

    df_1 = filter_window(df, mx, 1)
    df_12 = filter_window(df, mx, 12)

    assert df_12.count() >= df_1.count()


def test_detail_columns(spark, sample_silver):
    from src.faas4u.windows import normalize_inputs, get_max_month, filter_window
    from src.faas4u.metrics import compute_detailed_results

    df = normalize_inputs(sample_silver)
    mx = get_max_month(df)
    df_w = filter_window(df, mx, 3)

    dt = compute_detailed_results(df_w, 3, "test-run", datetime.now(), mx)
    pdf = dt.toPandas()

    assert "analytical_window_months" in pdf.columns
    assert "ape" in pdf.columns
    assert "smape_element" in pdf.columns  # intermediate column present
    assert "is_zero_actual" in pdf.columns
    assert pdf["analytical_window_months"].unique().tolist() == [3]


def test_leaderboard_ranking(spark, sample_silver):
    from src.faas4u.windows import normalize_inputs, get_max_month, filter_window
    from src.faas4u.metrics import compute_detailed_results, compute_performance_metrics, compute_leaderboard

    df = normalize_inputs(sample_silver)
    mx = get_max_month(df)
    df_w = filter_window(df, mx, 6)

    dt = compute_detailed_results(df_w, 6, "test-run", datetime.now(), mx)
    perf = compute_performance_metrics(dt, "test-run", datetime.now(), mx, 6)
    lb = compute_leaderboard(perf)
    pdf_lb = lb.toPandas()

    assert "rank_mape" in pdf_lb.columns
    assert pdf_lb["rank_mape"].min() == 1


# ── Windows Mode Tests ───────────────────────────────────────────────────────

def test_exec_mode_windows():
    """Default EXEC mode should produce [1, 3, 6, 12]."""
    from src.faas4u.config import ANALYTICAL_WINDOWS_EXEC
    assert ANALYTICAL_WINDOWS_EXEC == [1, 3, 6, 12]


def test_full_mode_windows():
    """FULL mode should produce [1, 2, ..., 24]."""
    from src.faas4u.config import ANALYTICAL_WINDOWS_FULL
    assert ANALYTICAL_WINDOWS_FULL == list(range(1, 25))
    assert 1 in ANALYTICAL_WINDOWS_FULL
    assert 24 in ANALYTICAL_WINDOWS_FULL
    assert len(ANALYTICAL_WINDOWS_FULL) == 24


def test_default_mode_is_exec():
    """Without env var override, default mode should be EXEC."""
    from src.faas4u.config import ANALYTICAL_WINDOWS_EXEC, ANALYTICAL_WINDOWS
    # In default test environment (no env var set), windows should be EXEC
    assert ANALYTICAL_WINDOWS == ANALYTICAL_WINDOWS_EXEC


# ── sMAPE Division by Zero Test ──────────────────────────────────────────────

def test_smape_zero_division(spark):
    """When both actual and forecast are 0, sMAPE element should be null."""
    from src.faas4u.metrics import compute_detailed_results

    rows = [
        {"sku_id": "Z1", "algo_name": "TestAlgo", "date_month": datetime(2024, 1, 1),
         "actual_qty": 0.0, "forecast_qty": 0.0},
        {"sku_id": "Z1", "algo_name": "TestAlgo", "date_month": datetime(2024, 2, 1),
         "actual_qty": 100.0, "forecast_qty": 90.0},
    ]
    df = spark.createDataFrame(pd.DataFrame(rows))
    dt = compute_detailed_results(df, 1, "test-run", datetime.now(), datetime(2024, 2, 1))
    pdf = dt.toPandas()

    # Row where both are 0: smape_element should be null
    zero_row = pdf[pdf["actual_qty"] == 0.0]
    assert zero_row["smape_element"].isna().all(), "sMAPE should be null when actual=0 and forecast=0"

    # Row where actual=100, forecast=90: smape_element should be non-null
    nonzero_row = pdf[pdf["actual_qty"] == 100.0]
    assert nonzero_row["smape_element"].notna().all(), "sMAPE should not be null for valid data"


def test_smape_in_perf_metrics(spark, sample_silver):
    """sMAPE should be present and reasonable in perf metrics."""
    from src.faas4u.windows import normalize_inputs, get_max_month, filter_window
    from src.faas4u.metrics import compute_detailed_results, compute_performance_metrics

    df = normalize_inputs(sample_silver)
    mx = get_max_month(df)
    df_w = filter_window(df, mx, 3)

    dt = compute_detailed_results(df_w, 3, "test-run", datetime.now(), mx)
    perf = compute_performance_metrics(dt, "test-run", datetime.now(), mx, 3)
    pdf = perf.toPandas()

    assert "smape" in pdf.columns
    assert "mae" in pdf.columns
    # sMAPE is a percentage, typically between 0 and 200
    smape_valid = pdf["smape"].dropna()
    assert (smape_valid >= 0).all()
    assert (smape_valid <= 200).all()
