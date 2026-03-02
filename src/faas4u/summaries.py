# =============================================================================
# FAAS4U — Summary Builder (for Claude prompts — NO raw data)
# =============================================================================
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def build_summary_dict(
    perf_df, fva_df, alerts_df, leaderboard_df,
) -> dict[str, Any]:
    """
    Extracts high-level aggregated KPIs from Pandas DataFrames (post-toPandas)
    for safe inclusion in Claude prompts. NO raw row data is included.
    """
    summary: dict[str, Any] = {"windows": {}}

    # Coerce to pandas if needed
    def _to_pd(df):
        try:
            return df.toPandas()
        except AttributeError:
            return df

    pdf_perf = _to_pd(perf_df)
    pdf_fva = _to_pd(fva_df)
    pdf_alerts = _to_pd(alerts_df)
    pdf_lb = _to_pd(leaderboard_df)

    for w in sorted(pdf_perf["analytical_window_months"].dropna().unique()):
        w = int(w)
        wp = pdf_perf[pdf_perf["analytical_window_months"] == w]
        wa = pdf_alerts[pdf_alerts["analytical_window_months"] == w]

        global_mape = float(wp["mape"].mean()) if not wp.empty else 0.0
        global_bias = float(wp["bias"].mean()) if not wp.empty else 0.0

        worst_products = (
            wp.groupby("product_id")["mape"].mean().nlargest(10).index.tolist()
        )
        worst_algos = (
            wp.groupby("algo_name")["mape"].mean().nlargest(3).index.tolist()
        )
        alert_counts = wa["severity"].value_counts().to_dict() if not wa.empty else {}

        wf = pdf_fva[pdf_fva["analytical_window_months"] == w]
        fva_dist = wf["fva_flag"].value_counts().to_dict() if not wf.empty else {}

        summary["windows"][w] = {
            "global_mape": round(global_mape, 2),
            "global_bias": round(global_bias, 2),
            "worst_products_top10": worst_products,
            "worst_algorithms_top3": worst_algos,
            "alerts_by_severity": alert_counts,
            "fva_distribution": fva_dist,
        }

    return summary
