# FAAS4U — Enterprise Forecast Validation Suite

> **Plateforme de validation de fiabilité et de performance des prévisions de forecasting intégrant les KPIs du Lead Time sur Microsoft Fabric.**
>
> PFE Salma | Mundiapolis 2026 — Encadrant : M. Kahlaoui

---

## 🏗️ Architecture Overview

FAAS4U follows the **Medallion Architecture** on Microsoft Fabric:

| Layer | Purpose |
|-------|---------|
| **Bronze** | Raw ingestion (CSV, ERP extracts) |
| **Silver** | Cleaned, normalized forecasts (`forecasts_clean`) |
| **Gold** | Validated KPIs, alerts, leaderboards, reports (8 tables) |

```
Bronze (raw) → Silver (clean) → Gold (validated KPIs + Claude narratives)
```

---

## 📊 Gold Tables (8 outputs)

| # | Table | Description |
|---|-------|-------------|
| 1 | `gld_algo_test_results_detail` | Row-level forecast vs actual (audit + BI) |
| 2 | `gld_performance_metrics` | MAPE / WAPE / RMSE / Bias / Tracking Signal per product × algo × window |
| 3 | `gld_fva_results` | Forecast Value Added: naïve → best algo → planner cascade |
| 4 | `gld_alertes_derive` | Drift alerts + Claude/fallback recommendations |
| 5 | `gld_algo_leaderboard` | Algorithm ranking by window × segment |
| 6 | `gld_leadtime_impact` | Safety stock & stockout risk assessment |
| 7 | `gld_dashboard_specs` | Power BI / Excel dashboard JSON specification |
| 8 | `gld_analytical_reports` | Executive Markdown reports (1-page + detailed) |

All Gold tables include `run_id`, `generated_at`, and `date_max_window` for pharma-grade audit traceability.

---

## 🔄 Multi-Horizon Analysis

Metrics are computed over **4 rolling analytical windows**: **1, 3, 6, and 12 months**, anchored at the most recent month available in the data.

- `analytical_window_months` = evaluation rolling window ∈ {1, 3, 6, 12}
- `horizon_mois` = forecast lead horizon (M+1, M+2, etc.) — **unchanged**

This enables time-sensitive accuracy tracking: short-term vs long-term model performance.

---

## ⏱️ Lead Time KPIs

The `gld_leadtime_impact` table evaluates stockout risk based on forecast volatility:

```
volatility = WAPE / 100     (WAPE is stored as percent 0–100, converted to ratio)
safety_stock_days = lead_time_days × volatility
ratio = safety_stock_days / lead_time_days
```

| Risk Severity | Ratio Threshold |
|--------------|----------------|
| LOW | ratio < 0.15 |
| MEDIUM | 0.15 ≤ ratio < 0.25 |
| HIGH | 0.25 ≤ ratio < 0.35 |
| CRITICAL | ratio ≥ 0.35 |

`stockout_risk_flag = True` when severity is HIGH or CRITICAL.

---

## 📈 Forecast Value Added (FVA)

FVA measures the value added by the human demand planner vs statistical algorithms:

```
FVA = algo_error − planner_error
```

- **Best Algo** is selected per product and analytical window (minimum WAPE).
- **Positive FVA** = planner improved accuracy (value added).
- **Negative FVA** = planner degraded accuracy (value lost).

---

## 🚀 How to Run

### Local Execution

```bash
# Install dependencies
pip install -r requirements.txt

# Run the validation engine (outputs CSV to ./outputs/)
python Notebooks/03_Validation_Engine.py

# Run tests
pytest -v tests/
```

### Enable Claude AI Narratives

```bash
# Windows
set ENABLE_CLAUDE=true
set ANTHROPIC_API_KEY=sk-...

# Linux/Mac
export ENABLE_CLAUDE=true
export ANTHROPIC_API_KEY=sk-...

python Notebooks/03_Validation_Engine.py
```

> When `ENABLE_CLAUDE=false` (default), all narrative outputs use deterministic fallback templates. The pipeline **never fails** due to missing API keys.

### Microsoft Fabric

Run `03_Validation_Engine.py` inside a Fabric Notebook cell. The engine auto-detects the Spark context and writes Delta tables to the **Fabric Lakehouse** (stored in OneLake).

To connect:
1. Fabric Workspace → Settings → Git Integration
2. Connect to your GitHub repository and branch
3. Sync the `FAAS4U_Fabric_Project/` folder

---

## 🧪 CI Testing

The test suite validates three critical areas:

```bash
pytest -v tests/
```

| Test File | Validates |
|-----------|-----------|
| `test_schemas.py` | Exact column names, column order, uniqueness constraints, audit trail completeness |
| `test_multi_horizon.py` | Window filtering logic, leaderboard ranking |
| `test_narratives_fallback.py` | Claude fallback templates (JSON + Markdown structure) |

> CI validates strict schemas + multi-horizon logic + Claude fallback behavior.

---

## 📁 Repository Structure

```
FAAS4U_Fabric_Project/
├── src/faas4u/          # Modular business logic
│   ├── config.py        # Thresholds, schemas, enforce_schema()
│   ├── windows.py       # Date normalization, window filtering
│   ├── metrics.py       # Detail, KPIs, leaderboard
│   ├── fva.py           # Forecast Value Added
│   ├── alerts.py        # Drift detection
│   ├── leadtime.py      # Safety stock & risk
│   ├── summaries.py     # Aggregated summary for Claude
│   ├── narratives.py    # Claude integration + fallbacks
│   └── io.py            # Delta / CSV writer
├── Notebooks/
│   └── 03_Validation_Engine.py  # Orchestrator
├── tests/               # pytest suite
├── outputs/             # Local CSV output (gitignored)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🔒 Security

- **No secrets committed.** API keys are read from environment variables only.
- `outputs/`, `.env`, `*.csv`, `*.log` are all gitignored.
- Claude prompts receive only aggregated summary data — never raw row-level data.
