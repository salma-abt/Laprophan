"""Quick verification script for FAAS4U updates (no Spark required)."""
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

def test_windows_mode_config():
    from src.faas4u.config import (
        ANALYTICAL_WINDOWS_EXEC, ANALYTICAL_WINDOWS_FULL,
        ANALYTICAL_WINDOWS, ANALYTICAL_WINDOWS_MODE,
    )
    assert ANALYTICAL_WINDOWS_EXEC == [1, 3, 6, 12]
    assert ANALYTICAL_WINDOWS_FULL == list(range(1, 25))
    assert ANALYTICAL_WINDOWS_MODE == "EXEC"
    assert ANALYTICAL_WINDOWS == [1, 3, 6, 12]
    print("PASS: Windows mode config")

def test_perf_schema_has_mae_smape():
    from src.faas4u.config import EXPECTED_SCHEMAS, TABLE_PERF_METRICS
    cols = EXPECTED_SCHEMAS[TABLE_PERF_METRICS]
    assert "mae" in cols
    assert "smape" in cols
    ri = cols.index("rmse")
    mi = cols.index("mae")
    si = cols.index("smape")
    bi = cols.index("bias")
    assert ri < mi < si < bi, f"Order wrong: rmse@{ri} mae@{mi} smape@{si} bias@{bi}"
    print(f"PASS: Perf schema: mae at idx {mi}, smape at idx {si}")

def test_leadtime_corr_schema():
    from src.faas4u.config import EXPECTED_SCHEMAS, TABLE_LEADTIME_CORR
    cols = EXPECTED_SCHEMAS[TABLE_LEADTIME_CORR]
    expected = [
        "analytical_window_months", "corr_metric_name", "correlation_value",
        "n_products", "date_max_window", "run_id", "generated_at",
    ]
    assert cols == expected
    print("PASS: Leadtime correlation schema")

def test_table_path():
    from src.faas4u.config import TABLE_LEADTIME_CORR
    assert "gld_leadtime_correlation" in TABLE_LEADTIME_CORR
    print(f"PASS: TABLE_LEADTIME_CORR = {TABLE_LEADTIME_CORR}")

def test_schema_count():
    from src.faas4u.config import EXPECTED_SCHEMAS
    names = [t.split("/")[-1] for t in EXPECTED_SCHEMAS.keys()]
    assert len(names) == 9, f"Expected 9 schemas, got {len(names)}"
    print(f"PASS: {len(names)} schemas: {names}")

def test_enforce_schema_pandas():
    import pandas as pd
    from src.faas4u.config import EXPECTED_SCHEMAS, TABLE_DETAIL, enforce_schema
    cols = EXPECTED_SCHEMAS[TABLE_DETAIL]
    pdf = pd.DataFrame({c: [1] for c in cols})
    result = enforce_schema(pdf, TABLE_DETAIL)
    assert list(result.columns) == cols
    print("PASS: enforce_schema works with Pandas")

def test_enforce_schema_rejects_extra():
    import pandas as pd
    from src.faas4u.config import EXPECTED_SCHEMAS, TABLE_DETAIL, enforce_schema
    cols = EXPECTED_SCHEMAS[TABLE_DETAIL]
    pdf = pd.DataFrame({c: [1] for c in cols})
    pdf["extra_col"] = 1
    try:
        enforce_schema(pdf, TABLE_DETAIL)
        assert False, "Should have raised ValueError"
    except ValueError:
        print("PASS: enforce_schema rejects extra columns")

def test_all_imports():
    from src.faas4u.leadtime import compute_leadtime_impact, compute_leadtime_correlation
    from src.faas4u.metrics import compute_detailed_results, compute_performance_metrics, compute_leaderboard
    from src.faas4u.fva import compute_fva
    from src.faas4u.alerts import compute_alerts
    from src.faas4u.io import write_output
    from src.faas4u.windows import filter_window, get_max_month, normalize_inputs
    print("PASS: All module imports successful")

if __name__ == "__main__":
    tests = [
        test_windows_mode_config,
        test_perf_schema_has_mae_smape,
        test_leadtime_corr_schema,
        test_table_path,
        test_schema_count,
        test_enforce_schema_pandas,
        test_enforce_schema_rejects_extra,
        test_all_imports,
    ]
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"FAIL: {t.__name__}: {e}")
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{len(tests)} passed")
    if passed == len(tests):
        print("ALL CHECKS PASSED")
    else:
        print(f"{len(tests)-passed} FAILED")
        sys.exit(1)
