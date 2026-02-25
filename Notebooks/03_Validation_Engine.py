# =============================================================================
# FAAS4U — Laprophan | Couche Gold
# Fichier  : 03_Validation_Engine.py
# Auteure  : Salma | PFE Mundiapolis 2026
# Encadrant: M. Kahlaoui
# Rôle     : Moteur de validation — Silver → Gold
#             → performance_metrics : MAPE/sMAPE/WAPE/MAE/RMSE/Biais/
#                                     Tracking Signal / Limited Theil's U
#             → fva_results         : Forecast Value Added humain vs algos
#             → alertes_derive      : Tracking Signal + recommandations Claude IA
# =============================================================================
# LOGIQUE MÉTIER :
#   Input  : forecasts_clean (Silver) — produit par 02_Bronze_to_Silver.py
#   Output : 3 tables Gold consommées par Power BI
#
# RÈGLES ABSOLUES :
#   - IQVIA = vérité terrain (ventes_reelles_qty)
#   - Métriques calculées par algo x segment_abcxyz x horizon (1-24 mois)
#   - FVA = comparaison naïve → algo → Expert_DP
#   - Tracking Signal hors seuil [-6, +6] = alerte automatique
#   - Audit trail sur chaque ligne (date_traitement)
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# 0. IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import functions as F # type: ignore
from pyspark.sql.window import Window # type: ignore
from pyspark.sql.types import DoubleType, StringType # type: ignore
from datetime import datetime
import requests
import json

# ─────────────────────────────────────────────────────────────────────────────
# 1. SESSION SPARK
# ─────────────────────────────────────────────────────────────────────────────
try:
    spark = SparkSession.builder.getOrCreate()
except:
    pass

# ─────────────────────────────────────────────────────────────────────────────
# 2. CHEMINS ONELAKE
# ─────────────────────────────────────────────────────────────────────────────
SILVER_PATH = "Tables/silver/"
GOLD_PATH   = "Tables/gold/"

TABLE_SILVER          = f"{SILVER_PATH}forecasts_clean"
TABLE_PERF_METRICS    = f"{GOLD_PATH}performance_metrics"
TABLE_FVA             = f"{GOLD_PATH}fva_results"
TABLE_ALERTES         = f"{GOLD_PATH}alertes_derive"

# Seuils métier
SEUIL_TRACKING_SIGNAL = 6.0      # Alerte si |TS| > 6 (standard industriel)
HORIZON_MAX           = 24        # Backtesting sur 1 à 24 mois
CLAUDE_API_KEY        = "VOTRE_CLE_API_CLAUDE"   # À remplacer / stocker en Key Vault

DATE_TRAITEMENT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────────────────────
# 3. CHARGEMENT SILVER
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ÉTAPE 1 — Chargement de forecasts_clean (Silver)")
print("="*60)

df_silver = spark.read.format("delta").load(TABLE_SILVER)

# On travaille uniquement sur les lignes avec actuals disponibles (backtesting)
df_bt = df_silver.filter(
    F.col("ventes_reelles_qty").isNotNull() &
    (F.col("ventes_reelles_qty") > 0)
)

print(f"  Total lignes Silver       : {df_silver.count()}")
print(f"  Lignes avec actuals IQVIA : {df_bt.count()} (utilisées pour backtesting)")

# ─────────────────────────────────────────────────────────────────────────────
# 4. CALCUL DU HORIZON DE PRÉVISION
# ─────────────────────────────────────────────────────────────────────────────
# Le horizon = nombre de mois entre la date d'émission du forecast et la période réelle.
# Si la colonne horizon_mois n'existe pas dans Silver, on la calcule depuis
# une colonne date_emission_forecast (date à laquelle le forecast a été généré).
# Sinon on utilise directement la colonne horizon_mois.

print("\n[INFO] Calcul des horizons de prévision...")

if "horizon_mois" in df_bt.columns:
    # Colonne déjà présente — on la filtre sur 1-24
    df_bt = df_bt.filter(
        (F.col("horizon_mois") >= 1) &
        (F.col("horizon_mois") <= HORIZON_MAX)
    )
elif "date_emission_forecast" in df_bt.columns:
    # Calcul depuis la date d'émission
    df_bt = df_bt.withColumn(
        "horizon_mois",
        F.months_between(F.col("periode"), F.col("date_emission_forecast")).cast("int")
    ).filter(
        (F.col("horizon_mois") >= 1) &
        (F.col("horizon_mois") <= HORIZON_MAX)
    )
else:
    # Fallback : horizon = 1 pour toutes les lignes (à corriger quand données réelles)
    print("  [ATTENTION] Colonne horizon_mois absente — horizon fixé à 1 par défaut")
    df_bt = df_bt.withColumn("horizon_mois", F.lit(1))

print(f"  [OK] Horizons calculés — plage : 1 à {HORIZON_MAX} mois")

# ─────────────────────────────────────────────────────────────────────────────
# 5. PRÉVISION NAÏVE (BASELINE)
# ─────────────────────────────────────────────────────────────────────────────
# La prévision naïve = ventes du mois précédent (random walk).
# C'est le benchmark minimum : tout algo doit faire mieux que ça.
# Utilisée pour : Limited Theil's U et FVA (point de départ de la cascade).

print("\n" + "="*60)
print("ÉTAPE 2 — Calcul de la prévision naïve (baseline)")
print("="*60)

# Fenêtre par SKU ordonnée par période
window_naive = Window.partitionBy("sku_id").orderBy("periode")

# Prévision naïve = ventes du mois M-1
df_naive = df_silver \
    .select("sku_id", "periode", "ventes_reelles_qty") \
    .dropDuplicates(["sku_id", "periode"]) \
    .withColumn(
        "forecast_naive",
        F.lag("ventes_reelles_qty", 1).over(window_naive)
    )

# Jointure avec df_bt
df_bt = df_bt.join(
    df_naive.select("sku_id", "periode", "forecast_naive"),
    on=["sku_id", "periode"],
    how="left"
)

print("  [OK] Prévision naïve calculée (lag M-1 sur IQVIA)")

# ─────────────────────────────────────────────────────────────────────────────
# 6. FONCTIONS DE MÉTRIQUES (UDFs et colonnes calculées)
# ─────────────────────────────────────────────────────────────────────────────
# Toutes les métriques sont calculées ligne à ligne d'abord,
# puis agrégées par (algorithme, segment_abcxyz, horizon_mois).

print("\n" + "="*60)
print("ÉTAPE 3 — Calcul des métriques ligne à ligne")
print("="*60)

# Colonnes intermédiaires sur df_bt
df_metrics_raw = df_bt \
    .withColumn(
        # ── Erreur absolue ──────────────────────────────────────────
        "abs_error",
        F.abs(F.col("forecast_qty") - F.col("ventes_reelles_qty"))
    ) \
    .withColumn(
        # ── Erreur signée (pour Biais) ──────────────────────────────
        "signed_error",
        F.col("forecast_qty") - F.col("ventes_reelles_qty")
    ) \
    .withColumn(
        # ── Erreur relative absolue (pour MAPE) ────────────────────
        # Protégée contre division par zéro
        "abs_pct_error",
        F.when(
            F.col("ventes_reelles_qty") > 0,
            F.abs(F.col("forecast_qty") - F.col("ventes_reelles_qty"))
            / F.col("ventes_reelles_qty") * 100
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        # ── sMAPE ──────────────────────────────────────────────────
        # Symmetric MAPE — entre 0 et 200%, symétrique
        "smape_val",
        F.when(
            (F.col("forecast_qty") + F.col("ventes_reelles_qty")) > 0,
            F.abs(F.col("forecast_qty") - F.col("ventes_reelles_qty"))
            / ((F.col("forecast_qty") + F.col("ventes_reelles_qty")) / 2) * 100
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        # ── Erreur au carré (pour RMSE) ─────────────────────────────
        "squared_error",
        F.pow(F.col("forecast_qty") - F.col("ventes_reelles_qty"), 2)
    ) \
    .withColumn(
        # ── Erreur naïve absolue (pour Theil's U) ──────────────────
        "abs_error_naive",
        F.when(
            F.col("forecast_naive").isNotNull() & (F.col("ventes_reelles_qty") > 0),
            F.abs(F.col("forecast_naive") - F.col("ventes_reelles_qty"))
        ).otherwise(F.lit(None))
    )

print("  [OK] Colonnes intermédiaires calculées : abs_error, signed_error,")
print("       abs_pct_error, smape_val, squared_error, abs_error_naive")

# ─────────────────────────────────────────────────────────────────────────────
# 7. AGRÉGATION → performance_metrics (TABLE GOLD 1)
# ─────────────────────────────────────────────────────────────────────────────
# Clé d'agrégation : algorithme x segment_abcxyz x horizon_mois
# Chaque ligne Gold = performance d'un algo sur un segment pour un horizon donné

print("\n" + "="*60)
print("ÉTAPE 4 — Agrégation → performance_metrics (Gold)")
print("="*60)

df_perf = df_metrics_raw.groupBy(
    "algorithme",
    "segment_abc",
    "segment_xyz",
    "segment_abcxyz",
    "horizon_mois"
).agg(

    # ── MAPE — Mean Absolute Percentage Error ──────────────────────
    # "De combien on se trompe en moyenne en %"
    F.mean("abs_pct_error").alias("mape"),

    # ── sMAPE — Symmetric MAPE ─────────────────────────────────────
    # Évite la pénalisation asymétrique haut/bas
    F.mean("smape_val").alias("smape"),

    # ── WAPE — Weighted Absolute Percentage Error ───────────────────
    # Pondéré par le volume — impact CA et taux de service
    # WAPE = somme(|erreurs|) / somme(actuals) * 100
    (F.sum("abs_error") / F.sum("ventes_reelles_qty") * 100).alias("wape"),

    # ── MAE — Mean Absolute Error ──────────────────────────────────
    # Erreur absolue en unités réelles — pour calibration production
    F.mean("abs_error").alias("mae"),

    # ── RMSE — Root Mean Square Error ──────────────────────────────
    # Pénalise les grosses erreurs — risque rupture/surstock
    F.sqrt(F.mean("squared_error")).alias("rmse"),

    # ── Biais — Forecast Bias ──────────────────────────────────────
    # Positif = sur-prévision systématique
    # Négatif = sous-prévision systématique
    F.mean("signed_error").alias("biais"),

    # ── Biais relatif (%) ──────────────────────────────────────────
    (F.sum("signed_error") / F.sum("ventes_reelles_qty") * 100).alias("biais_pct"),

    # ── Limited Theil's U ──────────────────────────────────────────
    # U < 1 : algo meilleur que naïf → valeur ajoutée
    # U = 1 : algo = naïf → inutile
    # U > 1 : algo pire que naïf → à rejeter
    (F.sqrt(F.mean("squared_error")) /
     F.sqrt(F.mean(F.pow(F.col("abs_error_naive"), 2)))
    ).alias("theil_u"),

    # ── Métriques complémentaires ───────────────────────────────────
    F.count("*").alias("nb_observations"),
    F.mean("ventes_reelles_qty").alias("volume_moyen_actuals"),
    F.stddev("ventes_reelles_qty").alias("volatilite_actuals"),

    # MAD — Mean Absolute Deviation (nécessaire pour Tracking Signal)
    F.mean("abs_error").alias("mad"),

    # Cumul des erreurs signées (nécessaire pour Tracking Signal)
    F.sum("signed_error").alias("cumul_erreurs_signees"),

).withColumn(
    # ── Tracking Signal ────────────────────────────────────────────
    # TS = cumul erreurs signées / MAD
    # Hors seuil [-6, +6] : alerte dérive progressive
    "tracking_signal",
    F.when(
        F.col("mad") > 0,
        F.col("cumul_erreurs_signees") / F.col("mad")
    ).otherwise(F.lit(0.0))

).withColumn(
    # ── Flag alerte Tracking Signal ────────────────────────────────
    "flag_derive",
    F.when(F.abs(F.col("tracking_signal")) > SEUIL_TRACKING_SIGNAL, True)
     .otherwise(False)

).withColumn(
    # ── Mape cible par segment ABC ─────────────────────────────────
    "mape_cible",
    F.when(F.col("segment_abc") == "A", 10.0)
     .when(F.col("segment_abc") == "B", 20.0)
     .otherwise(40.0)

).withColumn(
    # ── Flag dépassement cible MAPE ────────────────────────────────
    "flag_mape_hors_cible",
    F.when(F.col("mape") > F.col("mape_cible"), True).otherwise(False)

).withColumn(
    # ── Score global (rang de performance) ─────────────────────────
    # Combinaison MAPE + Biais normalisé — utilisé pour classement
    "score_global",
    F.round(
        (F.col("mape") * 0.5) +
        (F.abs(F.col("biais_pct")) * 0.3) +
        (F.col("theil_u") * 20 * 0.2),
        2
    )

).withColumn(
    "date_traitement", F.lit(DATE_TRAITEMENT)
)

# Arrondir toutes les métriques à 4 décimales
for col_name in ["mape", "smape", "wape", "mae", "rmse", "biais",
                  "biais_pct", "theil_u", "tracking_signal"]:
    df_perf = df_perf.withColumn(col_name, F.round(F.col(col_name), 4))

print(f"  performance_metrics : {df_perf.count()} lignes")
print(f"  Combinaisons        : algo x segment x horizon")

# Aperçu de validation
print("\n  Top 10 lignes (classées par MAPE croissant) :")
df_perf.orderBy("mape").select(
    "algorithme", "segment_abcxyz", "horizon_mois",
    "mape", "biais_pct", "theil_u", "flag_derive"
).show(10, truncate=False)

# Écriture Gold
df_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(TABLE_PERF_METRICS)

print(f"  [OK] performance_metrics écrit → {TABLE_PERF_METRICS}")

# ─────────────────────────────────────────────────────────────────────────────
# 8. CLASSEMENT DES ALGORITHMES PAR SEGMENT
# ─────────────────────────────────────────────────────────────────────────────
# Pour chaque (segment_abcxyz x horizon), on classe les algos du meilleur au pire.
# Le meilleur algo par segment = celui qu'on certifie pour ce segment.

print("\n" + "="*60)
print("ÉTAPE 5 — Classement des algorithmes par segment")
print("="*60)

window_rank = Window.partitionBy("segment_abcxyz", "horizon_mois").orderBy("mape")

df_classement = df_perf \
    .filter(F.col("algorithme") != "Expert_DP") \
    .withColumn("rang_algo", F.rank().over(window_rank)) \
    .withColumn(
        "certification",
        F.when(F.col("rang_algo") == 1, "CERTIFIE")
         .when(F.col("rang_algo") <= 3, "QUALIFIE")
         .otherwise("NON_QUALIFIE")
    )

print("  Meilleur algorithme certifié par segment (horizon 1 mois) :")
df_classement \
    .filter((F.col("rang_algo") == 1) & (F.col("horizon_mois") == 1)) \
    .select("segment_abcxyz", "algorithme", "mape", "certification") \
    .orderBy("segment_abcxyz") \
    .show(truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
# 9. FORECAST VALUE ADDED (FVA) → fva_results (TABLE GOLD 2)
# ─────────────────────────────────────────────────────────────────────────────
# Cascade FVA : Naïf → Algo → Expert_DP
#   FVA_algo = MAPE_naïf - MAPE_algo   (positif = algo ajoute de la valeur)
#   FVA_humain = MAPE_algo  - MAPE_DP  (positif = humain ajoute de la valeur)
#
# RÈGLE MÉTIER :
#   Si FVA_humain < 0 → alerte : le Demand Planner DÉGRADE la précision
#   Si FVA_humain > 0 → intervention conservée

print("\n" + "="*60)
print("ÉTAPE 6 — Forecast Value Added (FVA) → fva_results (Gold)")
print("="*60)

# 9.1 MAPE de la prévision naïve par segment et horizon
df_naive_mape = df_metrics_raw \
    .filter(F.col("forecast_naive").isNotNull()) \
    .withColumn(
        "abs_pct_error_naive",
        F.when(
            F.col("ventes_reelles_qty") > 0,
            F.abs(F.col("forecast_naive") - F.col("ventes_reelles_qty"))
            / F.col("ventes_reelles_qty") * 100
        ).otherwise(F.lit(None))
    ) \
    .groupBy("segment_abcxyz", "segment_abc", "segment_xyz", "horizon_mois") \
    .agg(F.mean("abs_pct_error_naive").alias("mape_naive")) \
    .withColumn("mape_naive", F.round(F.col("mape_naive"), 4))

# 9.2 MAPE du meilleur algorithme (hors Expert_DP) par segment et horizon
df_best_algo = df_perf \
    .filter(F.col("algorithme") != "Expert_DP") \
    .withColumn("rang", F.rank().over(window_rank)) \
    .filter(F.col("rang") == 1) \
    .select(
        "segment_abcxyz", "horizon_mois",
        F.col("algorithme").alias("meilleur_algo"),
        F.col("mape").alias("mape_best_algo"),
        F.col("biais_pct").alias("biais_best_algo"),
        F.col("theil_u").alias("theil_u_best_algo")
    )

# 9.3 MAPE du Demand Planner (Expert_DP)
df_dp_mape = df_perf \
    .filter(F.col("algorithme") == "Expert_DP") \
    .select(
        "segment_abcxyz", "horizon_mois",
        F.col("mape").alias("mape_dp"),
        F.col("biais_pct").alias("biais_dp"),
        F.col("tracking_signal").alias("ts_dp")
    )

# 9.4 Jointure des 3 niveaux
df_fva = df_naive_mape \
    .join(df_best_algo, on=["segment_abcxyz", "horizon_mois"], how="left") \
    .join(df_dp_mape,   on=["segment_abcxyz", "horizon_mois"], how="left") \
    .withColumn(
        # FVA algo = gain apporté par l'algorithme vs la baseline naïve
        "fva_algo_pts",
        F.round(F.col("mape_naive") - F.col("mape_best_algo"), 4)
    ) \
    .withColumn(
        # FVA humain = gain (ou perte) apporté par la correction du Demand Planner
        "fva_humain_pts",
        F.round(F.col("mape_best_algo") - F.col("mape_dp"), 4)
    ) \
    .withColumn(
        # FVA total = gain total vs baseline naïve
        "fva_total_pts",
        F.round(F.col("mape_naive") - F.col("mape_dp"), 4)
    ) \
    .withColumn(
        # Verdict sur la valeur ajoutée de l'algorithme
        "verdict_algo",
        F.when(F.col("fva_algo_pts") > 0, "VALEUR_AJOUTEE")
         .when(F.col("fva_algo_pts") == 0, "NEUTRE")
         .otherwise("DEGRADE")
    ) \
    .withColumn(
        # Verdict sur la valeur ajoutée du Demand Planner
        "verdict_humain",
        F.when(F.col("fva_humain_pts") > 2,  "HUMAIN_SURPERFORME")
         .when(F.col("fva_humain_pts") >= 0,  "HUMAIN_NEUTRE")
         .when(F.col("fva_humain_pts") >= -2, "HUMAIN_DEGRADE_LEGEREMENT")
         .otherwise("HUMAIN_DEGRADE_SIGNIFICATIVEMENT")
    ) \
    .withColumn(
        # Recommandation actionnable
        "recommandation",
        F.when(
            F.col("verdict_humain") == "HUMAIN_SURPERFORME",
            "Conserver les corrections DP — intelligence terrain validée"
        ).when(
            F.col("verdict_humain") == "HUMAIN_NEUTRE",
            "Corrections DP neutres — envisager suppression pour simplifier"
        ).when(
            F.col("verdict_humain") == "HUMAIN_DEGRADE_LEGEREMENT",
            "Corrections DP dégradent légèrement — former le Demand Planner"
        ).otherwise(
            "ALERTE : corrections DP dégradent significativement — bloquer les overrides"
        )
    ) \
    .withColumn(
        # Mape cible par segment
        "mape_cible",
        F.when(F.col("segment_abc") == "A", 10.0)
         .when(F.col("segment_abc") == "B", 20.0)
         .otherwise(40.0)
    ) \
    .withColumn(
        # Flag : le meilleur algo atteint-il la cible ?
        "flag_cible_atteinte",
        F.when(F.col("mape_best_algo") <= F.col("mape_cible"), True)
         .otherwise(False)
    ) \
    .withColumn("date_traitement", F.lit(DATE_TRAITEMENT))

print(f"  fva_results : {df_fva.count()} lignes")

# Aperçu FVA
print("\n  Aperçu FVA par segment (horizon 1 mois) :")
df_fva.filter(F.col("horizon_mois") == 1) \
    .select(
        "segment_abcxyz", "meilleur_algo",
        "mape_naive", "mape_best_algo", "mape_dp",
        "fva_algo_pts", "fva_humain_pts",
        "verdict_humain"
    ).orderBy("segment_abcxyz").show(truncate=False)

# Écriture Gold
df_fva.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(TABLE_FVA)

print(f"  [OK] fva_results écrit → {TABLE_FVA}")

# ─────────────────────────────────────────────────────────────────────────────
# 10. ALERTES ET RECOMMANDATIONS CLAUDE IA → alertes_derive (TABLE GOLD 3)
# ─────────────────────────────────────────────────────────────────────────────
# Deux types d'alertes :
#   1. Tracking Signal hors seuil [-6, +6] → dérive progressive détectée
#   2. MAPE hors cible par segment ABC     → performance insuffisante
#
# Pour chaque alerte, Claude IA génère une recommandation narrative.

print("\n" + "="*60)
print("ÉTAPE 7 — Alertes et recommandations Claude IA → alertes_derive (Gold)")
print("="*60)

# 10.1 Collecter toutes les alertes depuis performance_metrics
df_alertes_ts = df_perf \
    .filter(F.col("flag_derive") == True) \
    .select(
        "algorithme", "segment_abcxyz", "segment_abc", "horizon_mois",
        "mape", "biais_pct", "tracking_signal", "mape_cible",
        F.lit("TRACKING_SIGNAL").alias("type_alerte"),
        F.concat(
            F.lit("Tracking Signal = "),
            F.round(F.col("tracking_signal"), 2).cast("string"),
            F.lit(" (seuil ±6) — Biais cumulé détecté sur segment "),
            F.col("segment_abcxyz"),
            F.lit(", horizon "),
            F.col("horizon_mois").cast("string"),
            F.lit(" mois")
        ).alias("description_alerte")
    )

df_alertes_mape = df_perf \
    .filter(F.col("flag_mape_hors_cible") == True) \
    .select(
        "algorithme", "segment_abcxyz", "segment_abc", "horizon_mois",
        "mape", "biais_pct", "tracking_signal", "mape_cible",
        F.lit("MAPE_HORS_CIBLE").alias("type_alerte"),
        F.concat(
            F.lit("MAPE = "),
            F.round(F.col("mape"), 1).cast("string"),
            F.lit("% — dépasse la cible de "),
            F.col("mape_cible").cast("string"),
            F.lit("% pour segment "),
            F.col("segment_abcxyz"),
            F.lit(" (algorithme : "),
            F.col("algorithme"),
            F.lit(")")
        ).alias("description_alerte")
    )

# Union des deux types d'alertes
df_alertes_raw = df_alertes_ts.unionByName(df_alertes_mape)

nb_alertes = df_alertes_raw.count()
print(f"  Alertes détectées : {nb_alertes}")
print(f"    → Tracking Signal hors seuil : {df_alertes_ts.count()}")
print(f"    → MAPE hors cible            : {df_alertes_mape.count()}")

# 10.2 Génération des recommandations Claude IA
# Pour chaque alerte, on appelle l'API Claude pour générer une recommandation narrative.

def generer_recommandation_claude(description_alerte, segment, algorithme,
                                    mape, biais_pct, tracking_signal, mape_cible):
    """
    Appelle l'API Claude pour générer une recommandation narrative
    sur une dérive de performance détectée.
    """
    prompt = f"""Tu es un expert en Demand Planning pharmaceutique chez Laprophan (Maroc).
    
Analyse cette alerte de performance de forecast et génère une recommandation courte (3-4 phrases maximum), 
actionnable et précise pour le Demand Planner.

Contexte de l'alerte :
- Algorithme concerné : {algorithme}
- Segment ABC/XYZ : {segment}
- MAPE actuel : {mape}% (cible : {mape_cible}%)
- Biais relatif : {biais_pct}%
- Tracking Signal : {tracking_signal} (seuil ±6)
- Description : {description_alerte}

Recommandation (3-4 phrases, ton professionnel, en français) :"""

    try:
        headers = {
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        payload = {
            "model": "claude-sonnet-4-6",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}]
        }
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=payload,
            timeout=30
        )
        if response.status_code == 200:
            return response.json()["content"][0]["text"].strip()
        else:
            return f"[Recommandation indisponible — statut API : {response.status_code}]"
    except Exception as e:
        return f"[Recommandation indisponible — erreur : {str(e)}]"


# 10.3 Application des recommandations Claude IA
# On collecte les alertes en Python pour appeler l'API
alertes_list = df_alertes_raw.collect()

recommandations = []
for row in alertes_list:
    reco = generer_recommandation_claude(
        description_alerte = row["description_alerte"],
        segment            = row["segment_abcxyz"],
        algorithme         = row["algorithme"],
        mape               = row["mape"],
        biais_pct          = row["biais_pct"],
        tracking_signal    = row["tracking_signal"],
        mape_cible         = row["mape_cible"]
    )
    recommandations.append(reco)
    print(f"  [Claude IA] Alerte {row['type_alerte']} — {row['segment_abcxyz']} — recommandation générée")

# 10.4 Ajout des recommandations au DataFrame
# Conversion en DataFrame Spark
from pyspark.sql.types import StructType, StructField, StringType as ST # type: ignore

reco_data = [(r,) for r in recommandations]
df_reco = spark.createDataFrame(reco_data, ["recommandation_claude"])

# Ajout d'un index pour la jointure
from pyspark.sql.functions import monotonically_increasing_id # type: ignore
df_alertes_raw = df_alertes_raw.withColumn("_idx", monotonically_increasing_id())
df_reco        = df_reco.withColumn("_idx", monotonically_increasing_id())

df_alertes_final = df_alertes_raw.join(df_reco, on="_idx", how="left").drop("_idx")

# 10.5 Enrichissement final des alertes
df_alertes_final = df_alertes_final \
    .withColumn(
        # Niveau de criticité de l'alerte
        "criticite",
        F.when(
            (F.col("segment_abc") == "A") &
            (F.abs(F.col("tracking_signal")) > SEUIL_TRACKING_SIGNAL),
            "CRITIQUE"
        ).when(
            F.col("segment_abc") == "A",
            "ELEVEE"
        ).when(
            F.col("segment_abc") == "B",
            "MODEREE"
        ).otherwise("FAIBLE")
    ) \
    .withColumn(
        # Statut initial de l'alerte (workflow de traitement)
        "statut_alerte",
        F.lit("OUVERTE")
    ) \
    .withColumn(
        # Responsable de traitement selon RACI
        "responsable",
        F.when(F.col("criticite") == "CRITIQUE", "Directeur Supply Chain")
         .when(F.col("criticite") == "ELEVEE", "Demand Planner Senior")
         .otherwise("Demand Planner")
    ) \
    .withColumn("date_traitement", F.lit(DATE_TRAITEMENT)) \
    .withColumn("date_echeance",
        F.when(F.col("criticite") == "CRITIQUE",
               F.date_add(F.current_date(), 3))
         .when(F.col("criticite") == "ELEVEE",
               F.date_add(F.current_date(), 7))
         .otherwise(F.date_add(F.current_date(), 14))
    )

print(f"\n  alertes_derive : {df_alertes_final.count()} alertes")
print("\n  Répartition par criticité :")
df_alertes_final.groupBy("criticite").count().orderBy("criticite").show()

# Écriture Gold
df_alertes_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(TABLE_ALERTES)

print(f"  [OK] alertes_derive écrit → {TABLE_ALERTES}")

# ─────────────────────────────────────────────────────────────────────────────
# 11. RÉSUMÉ FINAL
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("RÉSUMÉ — 03_Validation_Engine.py")
print("="*60)
print(f"  Tables Gold produites :")
print(f"    → performance_metrics : MAPE/sMAPE/WAPE/MAE/RMSE/Biais/TS/Theil's U")
print(f"       par algo x segment_abcxyz x horizon 1-24 mois")
print(f"    → fva_results         : FVA cascade naïf → algo → Expert_DP")
print(f"       verdict humain + recommandation par segment")
print(f"    → alertes_derive      : {nb_alertes} alertes avec recommandations Claude IA")
print(f"       criticité + responsable + date d'échéance")
print(f"  Prochaine étape : 04_DAX_Measures.dax (Power BI scorecards)")
print("="*60 + "\n")

# =============================================================================
# FIN DU SCRIPT
# Ce script produit les 3 tables Gold consommées par Power BI :
#   performance_metrics → scorecards MAPE par algo/segment/horizon
#   fva_results         → comparaison humain vs algorithmes
#   alertes_derive      → monitoring dérives avec recommandations IA
# =============================================================================
