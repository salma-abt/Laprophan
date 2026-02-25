# =============================================================================
# FAAS4U — Laprophan | Couche Gold (Validation Engine)
# Fichier  : 03_Validation_Engine.py
# Auteure  : Salma | PFE Mundiapolis 2026
# Rôle     : Moteur de Backtesting PySpark
#             Calcul des 5 KPIs (MAPE, WAPE, Bias, Theil's U, Tracking Signal)
#             Évaluation FVA (Human vs Algo) par Segment ABC/XYZ
# =============================================================================
# LOGIQUE MÉTIER :
#   Compare les forecasts chargés (Silver) avec les réalisations IQVIA.
#   NE CRÉE PAS de prévisions, il se contente d'évaluer celles qui existent.
#   Génère des tables de résultats (Gold) prêtes pour Power BI.
#
# RÈGLES ABSOLUES :
#   - Période d'évaluation dynamique 
#   - BPF/GMP : Tracé et versionné
#   - Calculs exacts selon formules mathématiques Laprophan
# =============================================================================

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import functions as F # type: ignore
from pyspark.sql.window import Window # type: ignore
import datetime

# ─────────────────────────────────────────────────────────────────────────────
# 1. SESSION SPARK & CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
try:
    spark = SparkSession.builder.getOrCreate()
except:
    pass

SILVER_TABLE = "Tables/silver/forecasts_clean"
GOLD_KPI_TABLE = "Tables/gold/forecast_kpis"
GOLD_FVA_TABLE = "Tables/gold/fva_results"
GOLD_ALERTS_TABLE = "Tables/gold/stock_alerts"

DATE_TRAITEMENT = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
AUDIT_USER = "Salma (Fabric Sync)"

def add_gold_audit(df, step_name):
    return df \
        .withColumn("sys_gold_load_date", F.lit(DATE_TRAITEMENT)) \
        .withColumn("sys_gold_step", F.lit(step_name))

print("\n" + "="*60)
print("ÉTAPE 1 — Chargement de la table Silver (forecasts_clean)")
print("="*60)

# Chargement de la table Silver générée à l'étape précédente
df_silver = spark.read.format("delta").load(SILVER_TABLE)

# On filtre uniquement sur les lignes où on a des ventes réelles (pour backtest)
df_eval = df_silver.filter(F.col("ventes_reelles_qty").isNotNull() & (F.col("ventes_reelles_qty") > 0))

print(f"Lignes à évaluer (avec Actuals) : {df_eval.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. CALCUL DES KPIs DE BASE (Au niveau SKU / Période / Algo)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ÉTAPE 2 — Calcul des métriques de base (Error, Abs_Error)")
print("="*60)

df_eval = df_eval.withColumn(
    "error_qty", 
    F.col("forecast_qty") - F.col("ventes_reelles_qty")
).withColumn(
    "abs_error_qty", 
    F.abs(F.col("error_qty"))
)

# Pour Theil's U : On a besoin du "Naïve Forecast" (Prévision = Ventes réelles de la période précédente)
window_naive = Window.partitionBy("sku_id").orderBy("periode")
df_eval = df_eval.withColumn(
    "naive_forecast", 
    F.lag("ventes_reelles_qty", 1).over(window_naive)
)
# Erreur du modèle naïf: (Actual_t - Actual_t-1)
df_eval = df_eval.withColumn(
    "naive_error_qty", 
    F.col("ventes_reelles_qty") - F.col("naive_forecast")
)


# ─────────────────────────────────────────────────────────────────────────────
# 3. AGRÉGATION DES KPIs GLOBAUX (Par SKU et Algo sur tout l'historique)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ÉTAPE 3 — Agrégation des KPIs (MAPE, WAPE, Bias, Theil's U)")
print("="*60)

df_kpi = df_eval.groupBy(
    "sku_id", "algorithme", "segment_abc", "segment_xyz", "segment_abcxyz", "mape_cible_pct"
).agg(
    # BIAIS (Moyen)
    F.mean("error_qty").alias("bias_moyen"),
    F.sum("error_qty").alias("sum_errors"),
    
    # MAE (Mean Absolute Error)
    F.mean("abs_error_qty").alias("mae"),
    F.sum("abs_error_qty").alias("sum_abs_errors"),
    
    # Ventes totales sur la période évaluée
    F.sum("ventes_reelles_qty").alias("sum_actuals"),
    
    # Pre-calcul MAPE (moyenne des MAPEs individuels)
    F.mean(F.col("abs_error_qty") / F.col("ventes_reelles_qty")).alias("avg_mape_ratio"),
    
    # Pre-calcul Theil's U (Ratio des RMSE)
    F.sum(F.pow("error_qty", 2)).alias("sum_sq_errors"),
    F.sum(F.pow("naive_error_qty", 2)).alias("sum_sq_naive_errors")
)

# Finalisation des formules complexes
df_kpi = df_kpi.withColumn(
    "MAPE", F.round(F.col("avg_mape_ratio") * 100, 2)
).withColumn(
    "WAPE", 
    F.round(F.when(F.col("sum_actuals") > 0, (F.col("sum_abs_errors") / F.col("sum_actuals")) * 100).otherwise(None), 2)
).withColumn(
    # Tracking Signal = Sum of Errors / Mean Absolute Error (MAD)
    "Tracking_Signal", 
    F.round(F.when(F.col("mae") > 0, F.col("sum_errors") / F.col("mae")).otherwise(0), 2)
).withColumn(
    # Limited Theil's U = sqrt(sum(errors^2) / sum(naive_errors^2))
    # Si U < 1 : Modèle meilleur que prévision naïve
    # Si U > 1 : Modèle pire que prévision naïve
    "Theils_U",
    F.round(F.when(F.col("sum_sq_naive_errors") > 0, 
           F.sqrt(F.col("sum_sq_errors") / F.col("sum_sq_naive_errors"))).otherwise(None), 3)
)

# Classement global du meilleur Algo par SKU (Basé sur le WAPE)
window_rank = Window.partitionBy("sku_id").orderBy("WAPE")
df_kpi = df_kpi.withColumn("Rank_WAPE", F.rank().over(window_rank))
df_kpi = df_kpi.withColumn("Is_Best_Algo", F.when(F.col("Rank_WAPE") == 1, True).otherwise(False))

# Audit BPF
df_kpi = add_gold_audit(df_kpi, "Calcul_KPI_Performance")

# ─────────────────────────────────────────────────────────────────────────────
# 4. CALCUL DU FVA (Forecast Value Added)
# ─────────────────────────────────────────────────────────────────────────────
# FVA = Mesure la valeur apportée par l'humain (Expert DP) vs le meilleur algorithme,
# ou vs le modèle Naïf.
# Formule : FVA = Erreur(Naive) - Erreur(Expert) [ou WAPE(Meilleur Algo) - WAPE(Expert)]
# Un FVA positif indique que l'expert a amélioré la prévision.

print("\n" + "="*60)
print("ÉTAPE 4 — Calcul du FVA (Forecast Value Added)")
print("="*60)

# Isoler le WAPE de l'expert
df_expert = df_kpi.filter(F.col("algorithme") == "Expert_DP") \
                  .select("sku_id", F.col("WAPE").alias("WAPE_Expert"))

# Isoler le meilleur algorithme (hors expert)
df_best_algo = df_kpi.filter(F.col("algorithme") != "Expert_DP") \
                     .filter(F.col("Rank_WAPE") == 1) \
                     .select("sku_id", F.col("algorithme").alias("Best_Algo_Name"), 
                             F.col("WAPE").alias("WAPE_Best_Algo"))

df_fva = df_expert.join(df_best_algo, on="sku_id", how="inner")

# Calcul final du FVA
df_fva = df_fva.withColumn(
    "FVA_WAPE_Points", 
    F.round(F.col("WAPE_Best_Algo") - F.col("WAPE_Expert"), 2)
).withColumn(
    "Expert_Added_Value",
    F.when(F.col("FVA_WAPE_Points") > 0, "POSITIF (Amélioration)")
     .when(F.col("FVA_WAPE_Points") < 0, "NÉGATIF (Dégradation)")
     .otherwise("NEUTRE")
)

# On ramène la segmentation pour l'analyse FVA / Segment
df_fva = df_fva.join(df_kpi.select("sku_id", "segment_abcxyz").distinct(), on="sku_id", how="left")

df_fva = add_gold_audit(df_fva, "Calcul_FVA")

# ─────────────────────────────────────────────────────────────────────────────
# 5. GÉNÉRATION DES ALERTES (Tracking Signal Drift & Lead Time Impact)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ÉTAPE 5 — Génération des alertes métiers")
print("="*60)

# Alerte basée sur le Tracking Signal du meilleur Algorithme (+/- 4 est souvent la limite)
df_alerts = df_kpi.filter(F.col("Is_Best_Algo") == True) \
                  .select("sku_id", "segment_abcxyz", "algorithme", "Tracking_Signal", "bias_moyen", "mape_cible_pct", "MAPE")

# Récupération du Lead Time depuis la table originale Silver
df_lt_info = df_silver.select("sku_id", "lt_total_jours").distinct()
df_alerts = df_alerts.join(df_lt_info, on="sku_id", how="left")

# Logique d'Alerte
df_alerts = df_alerts.withColumn(
    "Alert_Tracking_Signal",
    F.when(F.col("Tracking_Signal") > 4, "ALERTE SURSTOCK (Biais Positif Continu)")
     .when(F.col("Tracking_Signal") < -4, "ALERTE RUPTURE (Biais Négatif Continu)")
     .otherwise("OK")
).withColumn(
    "Alert_Cible_MAPE",
    F.when(F.col("MAPE") > F.col("mape_cible_pct"), "NON ATTEINTE")
     .otherwise("ATTEINTE")
)

df_alerts = add_gold_audit(df_alerts, "Generation_Alertes")

# ─────────────────────────────────────────────────────────────────────────────
# 6. SAUVEGARDE EN FORMAT DELTA (ONELAKE)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("ÉTAPE 6 — Écriture des datamarts Gold")
print("="*60)

def save_gold(df, table_path):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    print(f"✅ Sauvegardé : {table_path} ({df.count()} lignes)")

save_gold(df_kpi, GOLD_KPI_TABLE)
save_gold(df_fva, GOLD_FVA_TABLE)
save_gold(df_alerts, GOLD_ALERTS_TABLE)

print("\n🎉 MOTEUR DE BACKTESTING (GOLD) TERMINÉ AVEC SUCCÈS.")
