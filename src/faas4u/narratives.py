# =============================================================================
# FAAS4U — Claude Narratives Module (with strict fallbacks)
# =============================================================================
from __future__ import annotations

import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

# Import config at function level to avoid circular imports and allow test overrides
def _get_claude_enabled() -> bool:
    return os.environ.get("ENABLE_CLAUDE", "false").lower() == "true"


def _get_api_key() -> str:
    return os.environ.get("ANTHROPIC_API_KEY", "")


def _load_ask_claude():
    """Dynamically imports ask_claude from utils.claude_helper."""
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        from utils.claude_helper import ask_claude
        return ask_claude
    except ImportError:
        logger.warning("claude_helper not found. Claude calls will fallback.")
        return None


def is_claude_enabled() -> bool:
    return _get_claude_enabled() and bool(_get_api_key())


def safe_claude_call(prompt: str, system_prompt: str = "") -> tuple[str, str]:
    """Low-level Claude call with error handling. Returns (text, source)."""
    if not is_claude_enabled():
        return "", "fallback"

    ask_claude = _load_ask_claude()
    if ask_claude is None:
        return "", "fallback"

    try:
        response = ask_claude(prompt=prompt, system_prompt=system_prompt, max_tokens=800)
        if "[ERREUR" in response or "[ÉCHEC" in response:
            logger.warning(f"Claude returned error text: {response[:100]}")
            return "", "fallback"
        return response.strip(), "claude"
    except Exception as e:
        logger.warning(f"Claude call failed: {e}")
        return "", "fallback"


# ── Alert Recommendations ────────────────────────────────────────────────────

def safe_claude_alert_recommendation(alert_context: dict) -> tuple[str, str]:
    """
    Returns (recommendation_text, model_source).
    Deterministic fallback if Claude is disabled or errors.
    """
    severity = alert_context.get("severity", "LOW")
    algo = alert_context.get("algo_name", "UNKNOWN")
    ts = float(alert_context.get("tracking_signal", 0.0))

    fallback = (
        f"Alerte de niveau {severity} détectée sur l'algorithme {algo}. "
        f"Le Tracking Signal est à {ts:.2f}. "
        "Actions recommandées:\n"
        "- Isoler les produits affectés et examiner les données récentes\n"
        "- Vérifier la saisonnalité et les biais cumulés\n"
        "- Recalibrer le modèle ou désactiver temporairement"
    )

    if not is_claude_enabled():
        return fallback, "fallback"

    prompt = (
        f"Alerte supply chain pharmaceutique.\n"
        f"Algo: {algo} | Criticité: {severity} | Tracking Signal: {ts:.2f}\n"
        f"Rédige 2 phrases exécutives puis exactement 3 bullet points 'Actions:'."
    )
    text, src = safe_claude_call(prompt, "Tu es analyste supply chain expert Laprophan.")
    return (text, src) if src == "claude" and text else (fallback, "fallback")


# ── Dashboard Specifications ─────────────────────────────────────────────────

_FALLBACK_DASHBOARD = {
    "pages": ["Overview", "Accuracy", "Algo Comparison", "FVA", "Risk", "Drift"],
    "visuals": [
        {"name": "MAPE Trend", "type": "line_chart"},
        {"name": "Algo Ranking", "type": "bar_chart"},
        {"name": "FVA Waterfall", "type": "waterfall"},
        {"name": "Risk Heatmap", "type": "matrix"},
    ],
    "measures": ["MAPE", "WAPE", "RMSE", "Bias", "Tracking Signal", "FVA Value"],
    "slicers": ["analytical_window_months", "algo_name", "segment_abcxyz", "product_id"],
    "thresholds": {"tracking_signal_abs": 6.0, "mape_red": 30.0},
    "tables_used": [
        "gld_performance_metrics", "gld_fva_results",
        "gld_alertes_derive", "gld_algo_leaderboard", "gld_leadtime_impact",
    ],
    "excel_pivots": ["Performance by Algorithm", "Lead Time Risk Matrix"],
    "notes": "Fallback spec — generated without Claude.",
}


def safe_claude_dashboard_spec(summary: dict) -> tuple[str, str]:
    """Returns (json_string, model_source)."""
    fallback_json = json.dumps(_FALLBACK_DASHBOARD)

    if not is_claude_enabled():
        return fallback_json, "fallback"

    prompt = (
        f"Architecte Power BI. Conçois un JSON STRICT pour dashboard FAAS4U.\n"
        f"Contexte: {json.dumps(summary)}\n"
        f"Clés obligatoires: pages, visuals, measures, slicers, thresholds, tables_used, excel_pivots.\n"
        f"Retourne UNIQUEMENT du JSON valide."
    )
    text, src = safe_claude_call(prompt)
    if src == "fallback" or not text:
        return fallback_json, "fallback"

    # Validate JSON structure
    try:
        clean = text.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]
        parsed = json.loads(clean)
        required = {"pages", "visuals", "measures", "slicers", "thresholds", "tables_used"}
        if not required.issubset(parsed.keys()):
            raise ValueError("Missing required keys")
        return json.dumps(parsed), "claude"
    except Exception as e:
        logger.warning(f"Claude dashboard JSON invalid: {e}")
        return fallback_json, "fallback"


# ── Analytical Reports ────────────────────────────────────────────────────────

def safe_claude_report(summary: dict, report_type: str) -> tuple[str, str]:
    """Returns (markdown_string, model_source)."""
    fallback = (
        f"# Rapport FAAS4U — {report_type}\n\n"
        f"## Synthèse Exécutive\n"
        f"Données évaluées sur {len(summary.get('windows', {}))} fenêtres analytiques.\n\n"
        f"## Analyse des Performances\nVoir les tables Gold pour le détail.\n\n"
        f"## Plan d'Action\nConsulter les alertes et recommandations.\n\n"
        f"*Généré par le fallback déterministe.*"
    )

    if not is_claude_enabled():
        return fallback, "fallback"

    prompt = (
        f"Rapport supply chain Markdown. Type: {report_type}.\n"
        f"Métriques: {json.dumps(summary)}\n"
        f"Sections obligatoires: # Synthèse Exécutive, # Analyse des Performances, # Plan d'Action"
    )
    text, src = safe_claude_call(prompt)
    if src == "fallback" or not text:
        return fallback, "fallback"

    # Structural validation
    if "# " not in text:
        logger.warning("Claude report lacks Markdown headings. Falling back.")
        return fallback, "fallback"

    return text.strip(), "claude"
