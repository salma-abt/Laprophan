# --- 02_Bronze_to_Silver.py ---
# Rôle : Couche Silver (CDC Section 4.1)
# Exigences : 5 Traitements (Nettoyage, Alignement, Ecarts Bruts, ABC/XYZ total, SANS filtre AX)

from pyspark.sql.functions import col, when, isnull, sum as _sum, date_trunc

print("1. Chargement des sources Bronze...")
df_mara = spark.table("BRONZE_SAP_MARA_MBEW")
df_sales = spark.table("BRONZE_SAP_SALES_ORDERS")
df_algos = spark.table("BRONZE_10_ALGOS_FORECASTS")
df_planner = spark.table("BRONZE_PLANNER_AUDIT_BPF")
df_iqvia = spark.table("BRONZE_IQVIA_SELLOUT")

print("2. Traitement 1 & 4 : Nettoyage et Segmentation Totale (Toutes classes conservées)...")
# On conserve explicitement tous les segments pour l'analyse globale avant la Phase 3 (AX seule)
df_mara_clean = df_mara.fillna({"Standard_Cost_MAD": 0, "Current_Stock_Qty": 0})
# Total Lead Time CDC (4 composantes)
df_mara_clean = df_mara_clean.withColumn(
    "Total_Lead_Time_Days", 
    col("LT_Besoins_Days") + col("LT_Fabrication_Days") + col("LT_QA_Days") + col("LT_Diffusion_Days")
)

print("3. Traitement 2 : Alignement Temporel (Truncate to Month)...")
# Aligner Factes et Prévisions sur le mois (Period)
df_sales_m = df_sales.withColumn("Period", date_trunc("month", col("Date"))).groupBy("Material_ID", "Period").agg(_sum("Order_Qty").alias("Actual_Order_Qty"))
df_iqvia_m = df_iqvia.withColumn("Period", date_trunc("month", col("Date"))).groupBy("Material_ID", "Period").agg(_sum("IQVIA_SellOut_Qty").alias("Actual_IQVIA_Qty"))
df_algos_m = df_algos.withColumn("Period", date_trunc("month", col("Target_Date"))).groupBy("Material_ID", "Period", "Algorithm").agg(_sum("Forecast_Qty").alias("Forecast_Qty"))
df_planner_m = df_planner.withColumn("Period", date_trunc("month", col("Target_Date"))).select("Material_ID", "Period", "Planner_Final_Qty", "Modified_By_User_ID", "Modification_Timestamp", "Reason_Code")

print("4. Jointures Universelles...")
# Créer une base consolidée MATNR + Period + Info Master Data
df_silver = df_sales_m.join(df_mara_clean, on="Material_ID", how="left")
df_silver = df_silver.join(df_iqvia_m, on=["Material_ID", "Period"], how="left")

# Joindre le bloc Planners pour avoir la Truth humaine
df_silver = df_silver.join(df_planner_m, on=["Material_ID", "Period"], how="left")

# Joindre les Algos (Attention: 1 ligne par matériel * période * algorithme)
df_silver_exploded = df_silver.join(df_algos_m, on=["Material_ID", "Period"], how="inner")

print("5. Traitement 3 : Calcul des Écarts Bruts...")
# Ecart Brut = Forecast Algo - Vente Réelle IQVIA (Source de vérité exigée CDC)
df_silver_exploded = df_silver_exploded.withColumn(
    "Ecart_Brut_Algo_vs_IQVIA",
    col("Forecast_Qty") - col("Actual_IQVIA_Qty")
).withColumn(
    "Ecart_Brut_Algo_vs_SAP",
    col("Forecast_Qty") - col("Actual_Order_Qty")
)

print("6. Sauvegarde Silver Layer (Tous segments conservés)...")
df_silver_exploded.write.format("delta").mode("overwrite").saveAsTable("SILVER_UNIFIED_FORECAST_DATA")

print("Traitements Silver CDC Section 4.1 terminés ! ✅ (Aucun filtre AX appliqué)")
