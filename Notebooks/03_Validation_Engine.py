# --- 03_Validation_Engine.py ---
# Rôle : Couche Gold (CDC Section 4.2)
# Exigences : 3 Tables avec 8 KPI (MAPE, RMSE, etc.) + FVA + Tracking Signal & Alertes
# Modèle Exécutif : PySpark SQL Context et calcul global pour la conformité

from pyspark.sql.functions import col, abs, when, sum as _sum, avg, count, sqrt, pow, lit
from pyspark.sql.window import Window

print("1. Chargement du socle unifié Silver...")
df_silver = spark.table("SILVER_UNIFIED_FORECAST_DATA")

# =========================================================
# TABLE 1 : PERFORMANCE METRICS (Les 8 KPI Officiels)
# =========================================================
# On travaille sur la base de la Vérité IQVIA demandée par le CDC
actual_col = "Actual_IQVIA_Qty"
forecast_col = "Forecast_Qty"

df_calc = df_silver.withColumn("Error", col(forecast_col) - col(actual_col))
df_calc = df_calc.withColumn("Abs_Error", abs(col("Error")))
# Pour le Theil's U (Nécessite le lag de la valeur actuelle sur une fenêtre temporelle)
windowSpec = Window.partitionBy("Material_ID", "Algorithm").orderBy("Period")
df_calc = df_calc.withColumn("Prev_Actual", lag(col(actual_col)).over(windowSpec))
df_calc = df_calc.withColumn("Theil_Denominator", pow(col(actual_col) - col("Prev_Actual"), 2))

# Agrégation des 8 Métriques par Algorithme & Produit
df_perf = df_calc.groupBy("Material_ID", "Product_Family", "ABC_Class", "XYZ_Class", "Algorithm").agg(
    # BIAIS (Positif = Surestimation)
    avg("Error").alias("Bias"),
    # MAE (Mean Absolute Error)
    avg("Abs_Error").alias("MAE"),
    # RMSE
    sqrt(avg(pow(col("Error"), 2))).alias("RMSE"),
    # MAPE (|F-A| / A)
    avg(when(col(actual_col) > 0, col("Abs_Error") / col(actual_col)).otherwise(0) * 100).alias("MAPE"),
    # sMAPE
    avg(when((col(actual_col) + abs(col(forecast_col))) > 0, 
             2 * col("Abs_Error") / (abs(col(forecast_col)) + col(actual_col))).otherwise(0) * 100).alias("sMAPE"),
    # WAPE (S_Abs_Error / S_Actual)
    (_sum("Abs_Error") / _sum(actual_col) * 100).alias("WAPE"),
    # Composantes du Tracking Signal et Theils'U
    _sum("Error").alias("Sum_Errors"),
    sqrt(_sum(pow("Error", 2)) / _sum("Theil_Denominator")).alias("Theils_U")
)

# Terminer le composant Tracking Signal = Cumul des Erreurs / MAD (Mean Absolute Deviation = MAE)
df_perf = df_perf.withColumn("Tracking_Signal", when(col("MAE") > 0, col("Sum_Errors") / col("MAE")).otherwise(0))

print("-> Création Table 1 : GOLD_PERFORMANCE_METRICS")
df_perf.write.format("delta").mode("overwrite").saveAsTable("GOLD_PERFORMANCE_METRICS")

# =========================================================
# TABLE 2 : FVA RESULTS (Humain vs Aglorythms)
# =========================================================
# Il s'agit de croiser l'erreur Planner avec les algos
df_planner_eval = df_calc.withColumn("Planner_Error", abs(col("Planner_Final_Qty") - col(actual_col)))
df_planner_eval = df_planner_eval.withColumn("Planner_MAPE", when(col(actual_col)>0, col("Planner_Error")/col(actual_col)*100).otherwise(0))

df_fva = df_planner_eval.groupBy("Material_ID", "Algorithm", "Reason_Code", "Modified_By_User_ID").agg(
    avg("Planner_MAPE").alias("MAPE_Humain"),
    avg(when(col(actual_col) > 0, col("Abs_Error") / col(actual_col)).otherwise(0) * 100).alias("MAPE_Algo"),
    (count(when(col("Planner_Final_Qty") != col("Actual_Order_Qty"), True)) / count("*")).alias("Override_Rate")
)

# FVA Humain vs Algo spécifique : MAPE_Algo - MAPE_Humain
df_fva = df_fva.withColumn("FVA_Humain_vs_Algo", col("MAPE_Algo") - col("MAPE_Humain"))

print("-> Création Table 2 : GOLD_FVA_RESULTS")
df_fva.write.format("delta").mode("overwrite").saveAsTable("GOLD_FVA_RESULTS")

# =========================================================
# TABLE 3 : ALERTES ET DERIVES (Risque Rupture, Drift, IA Recs)
# =========================================================
# Fusionner les data Master pour les Lead Times
df_alertes = df_perf.select("Material_ID", "Algorithm", "Tracking_Signal", "MAPE").join(
    df_calc.select("Material_ID", "Total_Lead_Time_Days", "Error").distinct(), on="Material_ID", how="left"
)

df_alertes = df_alertes.withColumn(
    "Tracking_Signal_Alert", 
    when(abs(col("Tracking_Signal")) > 4, lit("DERIVE_DETECTEE")).otherwise(lit("OK"))
)

# Simulation du système IA pour Reco NLP
df_alertes = df_alertes.withColumn(
    "Recommandation_Claude_IA",
    when(col("Tracking_Signal_Alert") == "DERIVE_DETECTEE", lit("Le Tracking Signal est critique. Vérifier les régresseurs de calendrier (Saisonnalité inexpliquée)."))
    .otherwise(lit("L'algorithme performe correctement dans les bornes autorisées."))
)

# Alerte Rupture = Erreur massive négative impactant le Lead Time
df_alertes = df_alertes.withColumn(
    "Alertes_Rupture_Surstock",
    when((col("Tracking_Signal_Alert") == "DERIVE_DETECTEE") & (col("Error") < 0), lit("DANGER RUPTURE : Sous-prévision flagrante sur flux matériel."))
    .when((col("Tracking_Signal_Alert") == "DERIVE_DETECTEE") & (col("Error") > 0), lit("DANGER SURSTOCK : Immobilisation financière."))
    .otherwise(lit("STABLE"))
)

print("-> Création Table 3 : GOLD_ALERTES_DERIVE")
df_alertes.write.format("delta").mode("overwrite").saveAsTable("GOLD_ALERTES_DERIVE")

print("Gold Layer CDC 4.2 complétée ! ✅")
