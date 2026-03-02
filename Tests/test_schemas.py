# =============================================================================
# FAAS4U — Strict Schema Tests
# Validates exact column names AND column order for all 9 Gold tables.
# =============================================================================
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime

import pytest

# Ensure project root is findable
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession  # type: ignore

        s = SparkSession.builder.master("local[1]") \
            .appName("FAAS4U_SchemaTest") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .getOrCreate()
        s.sparkContext.setLogLevel("ERROR")
        yield s
        s.stop()
    except Exception as e:
        pytest.skip(f"PySpark not available: {e}")


@pytest.fixture(scope="module")
def pipeline_outputs(spark):
    """Run the pipeline on a deterministic mock dataset and return all outputs."""
    import pandas as pd
    import numpy as np
    from src.faas4u.config import (
        ANALYTICAL_WINDOWS, TABLE_DETAIL, TABLE_PERF_METRICS,
        TABLE_FVA, TABLE_ALERTES, TABLE_LEADERBOARD, TABLE_LEADTIME,
        TABLE_LEADTIME_CORR, enforce_schema,
    )
    from src.faas4u.windows import normalize_inputs, get_max_month, filter_window
    from src.faas4u.metrics import compute_detailed_results, compute_performance_metrics, compute_leaderboard
    from src.faas4u.fva import compute_fva
    from src.faas4u.alerts import compute_alerts
    from src.faas4u.leadtime import compute_leadtime_impact, compute_leadtime_correlation

    # ── Build deterministic mock ─────────────────────────────────────────────
    np.random.seed(42)
    months = pd.date_range("2023-01-01", periods=12, freq="MS").tolist()
    rows = []
    for sku in ["P1", "P2"]:
        for algo in ["A1", "A2", "naive", "Expert_DP"]:
            for m in months:
                actual = float(np.random.randint(80, 200))
                # A1 is better than A2 for P1; opposite for P2
                if algo == "A1":
                    noise = 10 if sku == "P1" else 40
                elif algo == "A2":
                    noise = 40 if sku == "P1" else 10
                elif algo == "naive":
                    noise = 50
                else:
                    noise = 25
                forecast = actual + float(np.random.randint(-noise, noise))
                rows.append({
                    "sku_id": sku,
                    "algorithme": algo,
                    "periode": m,
                    "ventes_reelles_qty": actual,
                    "forecast_qty": forecast,
                    "segment_abcxyz": "AX" if sku == "P1" else "BY",
                    "horizon_mois": 1,
                })

    df_silver = spark.createDataFrame(pd.DataFrame(rows))
    
    run_id = str(uuid.uuid4())
    generated_at = datetime.now()

    df_norm = normalize_inputs(df_silver)
    max_month = get_max_month(df_norm)

    results: dict[str, object] = {}
    acc = {}

    def _union(key, df):
        acc[key] = df if key not in acc or acc[key] is None else acc[key].unionByName(df)

    for w in ANALYTICAL_WINDOWS:
        df_w = filter_window(df_norm, max_month, w)
        if df_w.rdd.isEmpty():
            continue
        dt = compute_detailed_results(df_w, w, run_id, generated_at, max_month)

        # Strip smape_element before accumulating detail (not in enforced schema)
        dt_clean = dt.drop("smape_element")
        _union(TABLE_DETAIL, dt_clean)

        perf = compute_performance_metrics(dt, run_id, generated_at, max_month, w)
        _union(TABLE_PERF_METRICS, perf)
        fva = compute_fva(perf)
        _union(TABLE_FVA, fva)
        alerts = compute_alerts(perf)
        _union(TABLE_ALERTES, alerts)
        lb = compute_leaderboard(perf)
        _union(TABLE_LEADERBOARD, lb)
        lt = compute_leadtime_impact(perf, run_id, generated_at, max_month)
        _union(TABLE_LEADTIME, lt)

        # Lead Time Correlation
        lt_corr = compute_leadtime_correlation(lt, perf, w, run_id, generated_at, max_month)
        _union(TABLE_LEADTIME_CORR, lt_corr)

    results.update(acc)
    return results


# ── Schema Tests ─────────────────────────────────────────────────────────────

from src.faas4u.config import (
    EXPECTED_SCHEMAS, TABLE_DETAIL, TABLE_PERF_METRICS, TABLE_FVA,
    TABLE_ALERTES, TABLE_LEADERBOARD, TABLE_LEADTIME, TABLE_LEADTIME_CORR,
)


class TestDetailSchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_DETAIL]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_DETAIL]

    def test_windows(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_DETAIL].toPandas()
        assert set(df["analytical_window_months"].unique()) == {1, 3, 6, 12}


class TestPerfMetricsSchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_PERF_METRICS]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_PERF_METRICS]

    def test_has_mae_and_smape(self, pipeline_outputs):
        """Verify MAE and sMAPE are present and are numeric."""
        pdf = pipeline_outputs[TABLE_PERF_METRICS].toPandas()
        assert "mae" in pdf.columns
        assert "smape" in pdf.columns
        # MAE should be non-negative for all rows
        assert (pdf["mae"].dropna() >= 0).all()

    def test_n_obs_decreases_with_window(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_PERF_METRICS].toPandas()
        n12 = pdf[pdf["analytical_window_months"] == 12]["n_obs"].sum()
        n1 = pdf[pdf["analytical_window_months"] == 1]["n_obs"].sum()
        assert n12 >= n1


class TestFVASchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_FVA]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_FVA]


class TestAlertesSchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_ALERTES]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_ALERTES]

    def test_severity_values(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_ALERTES].toPandas()
        if not pdf.empty:
            assert set(pdf["severity"].unique()).issubset({"LOW", "MEDIUM", "HIGH", "CRITICAL"})


class TestLeaderboardSchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_LEADERBOARD]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_LEADERBOARD]

    def test_ranking_exists(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_LEADERBOARD].toPandas()
        assert "rank_mape" in pdf.columns
        assert pdf["rank_mape"].min() == 1


class TestLeadtimeSchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_LEADTIME]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_LEADTIME]

    def test_one_row_per_product_window(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_LEADTIME].toPandas()
        dupes = pdf.duplicated(subset=["product_id", "analytical_window_months"])
        assert not dupes.any(), "Duplicate (product_id, window) in leadtime!"

    def test_risk_severity_values(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_LEADTIME].toPandas()
        allowed = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        assert set(pdf["risk_severity"].unique()).issubset(allowed)

    def test_audit_not_null(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_LEADTIME].toPandas()
        assert not pdf["run_id"].isna().any()
        assert not pdf["generated_at"].isna().any()
        assert not pdf["date_max_window"].isna().any()


class TestLeadtimeCorrelationSchema:
    def test_columns_exact(self, pipeline_outputs):
        df = pipeline_outputs[TABLE_LEADTIME_CORR]
        assert list(df.columns) == EXPECTED_SCHEMAS[TABLE_LEADTIME_CORR]

    def test_corr_metric_names(self, pipeline_outputs):
        """Verify correlation is computed for wape and smape."""
        pdf = pipeline_outputs[TABLE_LEADTIME_CORR].toPandas()
        assert set(pdf["corr_metric_name"].unique()) == {"wape", "smape"}

    def test_rows_per_window(self, pipeline_outputs):
        """Each window should produce exactly 2 rows (wape + smape)."""
        pdf = pipeline_outputs[TABLE_LEADTIME_CORR].toPandas()
        for w, grp in pdf.groupby("analytical_window_months"):
            assert len(grp) == 2, f"Window {w} has {len(grp)} rows, expected 2"

    def test_audit_not_null(self, pipeline_outputs):
        pdf = pipeline_outputs[TABLE_LEADTIME_CORR].toPandas()
        assert not pdf["run_id"].isna().any()
        assert not pdf["generated_at"].isna().any()
        assert not pdf["date_max_window"].isna().any()
