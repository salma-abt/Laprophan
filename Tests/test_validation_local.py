import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType
from datetime import datetime

# Initialize local Spark session
spark = SparkSession.builder \
    .appName("FAAS4U_Local_Test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

LOCAL_DATA_DIR = r"C:\Users\H P\Documents\Laprophan\Data_Test"
SILVER_FILE = os.path.join(LOCAL_DATA_DIR, "forecasts_clean.csv")
GOLD_KPI_FILE = os.path.join(LOCAL_DATA_DIR, "performance_metrics_test")
GOLD_FVA_FILE = os.path.join(LOCAL_DATA_DIR, "fva_results_test")
GOLD_ALERTES_FILE = os.path.join(LOCAL_DATA_DIR, "alertes_derive_test")

print("=======================================================================")
print(f"ÉTAPE 1 — Chargement Local de forecasts_clean depuis : {SILVER_FILE}")
print("=======================================================================")

df_silver = spark.read.csv(SILVER_FILE, header=True, inferSchema=True)

# Cast some columns that might have been read as strings
df_silver = df_silver.withColumn("periode", F.to_date(F.col("periode"))) \
    .withColumn("ventes_reelles_qty", F.col("ventes_reelles_qty").cast("double")) \
    .withColumn("forecast_qty", F.col("forecast_qty").cast("double"))

df_bt = df_silver.filter(
    F.col("ventes_reelles_qty").isNotNull() &
    (F.col("ventes_reelles_qty") > 0)
)
df_bt = df_bt.withColumn("horizon_mois", F.lit(1))

# === LOGIC FROM 03_Validation_Engine.py ===
HORIZON_MAX = 24
SEUIL_TRACKING_SIGNAL = 6.0
DATE_TRAITEMENT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

window_naive = Window.partitionBy("sku_id").orderBy("periode")
df_naive = df_silver \
    .select("sku_id", "periode", "ventes_reelles_qty") \
    .dropDuplicates(["sku_id", "periode"]) \
    .withColumn("forecast_naive", F.lag("ventes_reelles_qty", 1).over(window_naive))

df_bt = df_bt.join(df_naive.select("sku_id", "periode", "forecast_naive"), on=["sku_id", "periode"], how="left")

df_metrics_raw = df_bt \
    .withColumn("abs_error", F.abs(F.col("forecast_qty") - F.col("ventes_reelles_qty"))) \
    .withColumn("signed_error", F.col("forecast_qty") - F.col("ventes_reelles_qty")) \
    .withColumn("abs_pct_error", F.when(F.col("ventes_reelles_qty") > 0, F.abs(F.col("forecast_qty") - F.col("ventes_reelles_qty")) / F.col("ventes_reelles_qty") * 100).otherwise(F.lit(None))) \
    .withColumn("smape_val", F.when((F.col("forecast_qty") + F.col("ventes_reelles_qty")) > 0, F.abs(F.col("forecast_qty") - F.col("ventes_reelles_qty")) / ((F.col("forecast_qty") + F.col("ventes_reelles_qty")) / 2) * 100).otherwise(F.lit(None))) \
    .withColumn("squared_error", F.pow(F.col("forecast_qty") - F.col("ventes_reelles_qty"), 2)) \
    .withColumn("abs_error_naive", F.when(F.col("forecast_naive").isNotNull() & (F.col("ventes_reelles_qty") > 0), F.abs(F.col("forecast_naive") - F.col("ventes_reelles_qty"))).otherwise(F.lit(None)))

print("\n--- ÉTAPE 4 : Calcul KPIs ---")
df_perf = df_metrics_raw.groupBy("algorithme", "segment_abc", "segment_xyz", "segment_abcxyz", "horizon_mois").agg(
    F.mean("abs_pct_error").alias("mape"),
    F.mean("smape_val").alias("smape"),
    (F.sum("abs_error") / F.sum("ventes_reelles_qty") * 100).alias("wape"),
    F.mean("abs_error").alias("mae"),
    F.sqrt(F.mean("squared_error")).alias("rmse"),
    F.mean("signed_error").alias("biais"),
    (F.sum("signed_error") / F.sum("ventes_reelles_qty") * 100).alias("biais_pct"),
    (F.sqrt(F.mean("squared_error")) / F.sqrt(F.mean(F.pow(F.col("abs_error_naive"), 2)))).alias("theil_u"),
    F.count("*").alias("nb_observations"),
    F.mean("ventes_reelles_qty").alias("volume_moyen_actuals"),
    F.stddev("ventes_reelles_qty").alias("volatilite_actuals"),
    F.mean("abs_error").alias("mad"),
    F.sum("signed_error").alias("cumul_erreurs_signees"),
).withColumn("tracking_signal", F.when(F.col("mad") > 0, F.col("cumul_erreurs_signees") / F.col("mad")).otherwise(F.lit(0.0)))\
 .withColumn("flag_derive", F.when(F.abs(F.col("tracking_signal")) > SEUIL_TRACKING_SIGNAL, True).otherwise(False))\
 .withColumn("mape_cible", F.when(F.col("segment_abc") == "A", 10.0).when(F.col("segment_abc") == "B", 20.0).otherwise(40.0))\
 .withColumn("flag_mape_hors_cible", F.when(F.col("mape") > F.col("mape_cible"), True).otherwise(False))

for col_name in ["mape", "smape", "wape", "mae", "rmse", "biais", "biais_pct", "theil_u", "tracking_signal"]:
    df_perf = df_perf.withColumn(col_name, F.round(F.col(col_name), 4))

print("--> KPIs Calculés !")
df_perf.orderBy("mape").show(5)

window_rank = Window.partitionBy("segment_abcxyz", "horizon_mois").orderBy("mape")

print("\n--- ÉTAPE 6 : Calcul FVA ---")
df_naive_mape = df_metrics_raw.filter(F.col("forecast_naive").isNotNull()).withColumn("abs_pct_error_naive", F.when(F.col("ventes_reelles_qty") > 0, F.abs(F.col("forecast_naive") - F.col("ventes_reelles_qty")) / F.col("ventes_reelles_qty") * 100).otherwise(F.lit(None))).groupBy("segment_abcxyz", "segment_abc", "segment_xyz", "horizon_mois").agg(F.mean("abs_pct_error_naive").alias("mape_naive")).withColumn("mape_naive", F.round(F.col("mape_naive"), 4))
df_best_algo = df_perf.filter(F.col("algorithme") != "Expert_DP").withColumn("rang", F.rank().over(window_rank)).filter(F.col("rang") == 1).select("segment_abcxyz", "horizon_mois", F.col("algorithme").alias("meilleur_algo"), F.col("mape").alias("mape_best_algo"))
df_dp_mape = df_perf.filter(F.col("algorithme") == "Expert_DP").select("segment_abcxyz", "horizon_mois", F.col("mape").alias("mape_dp"))

df_fva = df_naive_mape.join(df_best_algo, on=["segment_abcxyz", "horizon_mois"], how="left").join(df_dp_mape, on=["segment_abcxyz", "horizon_mois"], how="left").withColumn("fva_algo_pts", F.round(F.col("mape_naive") - F.col("mape_best_algo"), 4)).withColumn("fva_humain_pts", F.round(F.col("mape_best_algo") - F.col("mape_dp"), 4)).withColumn("verdict_humain", F.when(F.col("fva_humain_pts") > 2, "HUMAIN_SURPERFORME").when(F.col("fva_humain_pts") >= 0, "HUMAIN_NEUTRE").when(F.col("fva_humain_pts") >= -2, "HUMAIN_DEGRADE_LEGEREMENT").otherwise("HUMAIN_DEGRADE_SIGNIFICATIVEMENT"))

print("--> FVA Calculé !")
df_fva.show(5)

print("\n--- SAUVEGARDE LOCALE CSV POUR VERIFICATION ---")
df_perf.repartition(1).write.csv(GOLD_KPI_FILE, header=True, mode="overwrite")
df_fva.repartition(1).write.csv(GOLD_FVA_FILE, header=True, mode="overwrite")

print(f"✅ Fichiers sauvegardés avec succès dans : {LOCAL_DATA_DIR}")
print("   - performance_metrics_test/")
print("   - fva_results_test/")
