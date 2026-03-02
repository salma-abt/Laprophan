# =============================================================================
# FAAS4U — Claude Fallback Tests
# Validates all narrative functions return valid fallback when ENABLE_CLAUDE=false.
# =============================================================================
from __future__ import annotations

import json
import os
import sys

import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


@pytest.fixture(autouse=True)
def disable_claude():
    """Ensure Claude is disabled for these tests."""
    os.environ["ENABLE_CLAUDE"] = "false"
    os.environ.pop("ANTHROPIC_API_KEY", None)
    yield
    os.environ.pop("ENABLE_CLAUDE", None)


class TestAlertFallback:
    def test_returns_fallback_source(self):
        from src.faas4u.narratives import safe_claude_alert_recommendation

        ctx = {"severity": "CRITICAL", "algo_name": "Prophet", "tracking_signal": 8.5}
        text, src = safe_claude_alert_recommendation(ctx)
        assert src == "fallback"
        assert "CRITICAL" in text
        assert "8.50" in text
        assert len(text) > 20

    def test_low_severity(self):
        from src.faas4u.narratives import safe_claude_alert_recommendation

        ctx = {"severity": "LOW", "algo_name": "ETS", "tracking_signal": 2.1}
        text, src = safe_claude_alert_recommendation(ctx)
        assert src == "fallback"
        assert "LOW" in text


class TestDashboardSpecFallback:
    def test_returns_valid_json(self):
        from src.faas4u.narratives import safe_claude_dashboard_spec

        js, src = safe_claude_dashboard_spec({})
        assert src == "fallback"

        parsed = json.loads(js)
        required = {"pages", "visuals", "measures", "slicers", "thresholds", "tables_used"}
        assert required.issubset(parsed.keys())

    def test_json_has_excel_pivots(self):
        from src.faas4u.narratives import safe_claude_dashboard_spec

        js, _ = safe_claude_dashboard_spec({})
        parsed = json.loads(js)
        assert "excel_pivots" in parsed
        assert isinstance(parsed["excel_pivots"], list)


class TestReportFallback:
    def test_monthly_1page(self):
        from src.faas4u.narratives import safe_claude_report

        md, src = safe_claude_report({"windows": {1: {}}}, "monthly_1page")
        assert src == "fallback"
        assert "# Rapport FAAS4U" in md
        assert "Synthèse Exécutive" in md

    def test_monthly_detailed(self):
        from src.faas4u.narratives import safe_claude_report

        md, src = safe_claude_report({}, "monthly_detailed")
        assert src == "fallback"
        assert "fallback déterministe" in md

    def test_report_not_empty(self):
        from src.faas4u.narratives import safe_claude_report

        md, _ = safe_claude_report({}, "monthly_1page")
        assert len(md) > 50


class TestIsClaudeEnabled:
    def test_disabled_by_default(self):
        from src.faas4u.narratives import is_claude_enabled

        assert is_claude_enabled() is False
