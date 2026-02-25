# --- 03_Forecasting_Engine.py ---
# Rôle : Moteur de backtesting IA & Calcul "Lead Time Impact" (Phase 2 - Couche Gold)
# Objectif : Paralléliser Prophet & Holt-Winters, lier l'erreur de prévision au Lead Time
# Conformité : BPF (Audit Trail propagé) & Métier (Supply Chain - Segment AX)

import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType, col, abs, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from prophet import Prophet
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()

print("1. Chargement des données Silver (Segment AX exclusif)...")
df_sales_ax = spark.table("SAP_SILVER_SALES_AX")
df_forecast_ax = spark.table("SAP_SILVER_FORECASTS_AUDIT_AX")
# On ramène la MAster Data pour croiser les Lead Times de Production/Achat
df_mara_clean = spark.table("SAP_BRONZE_MARA_MARC") # Idéalement pris depuis Silver si dispo, ou Bronze

# =========================================================
# ÉTAPE A : MOTEUR ALGORITHMIQUE VIA PANDAS UDF (SPARK)
# =========================================================
result_schema = StructType([
    StructField("Material_ID", StringType(), True),
    StructField("Sales_Date", DateType(), True),
    StructField("Actual_Qty", FloatType(), True),
    StructField("Forecast_Prophet", FloatType(), True),
    StructField("Forecast_HoltWinters", FloatType(), True)
])

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def run_backtesting_udf(df):
    """
    Entraîne Prophet et Holt-Winters sur l'historique d'un produit.
    Effectue un backtest sur les 6 derniers mois.
    """
    material_id = df["Material_ID"].iloc[0]
    df = df.sort_values("Sales_Date")
    
    test_horizon = 6
    train_size = len(df) - test_horizon
    
    if train_size < 12: 
        return pd.DataFrame() 
        
    train = df.iloc[:train_size]
    test = df.iloc[train_size:]
    
    # Modèle 1 : Prophet
    prophet_df = train[["Sales_Date", "Quantity_Sold"]].rename(columns={"Sales_Date": "ds", "Quantity_Sold": "y"})
    m = Prophet(daily_seasonality=False, weekly_seasonality=False, yearly_seasonality=True)
    try:
        m.fit(prophet_df)
        future = m.make_future_dataframe(periods=test_horizon, freq='MS')
        forecast_prophet = m.predict(future)
        pred_prophet = forecast_prophet["yhat"].iloc[-test_horizon:].values
        pred_prophet = [max(0, p) for p in pred_prophet] 
    except:
        pred_prophet = [train["Quantity_Sold"].mean()] * test_horizon 
        
    # Modèle 2 : Holt-Winters
    try:
        hw_model = ExponentialSmoothing(train["Quantity_Sold"], seasonal_periods=12, trend='add', seasonal='add').fit(optimized=True)
        pred_hw = hw_model.forecast(test_horizon).values
        pred_hw = [max(0, p) for p in pred_hw]
    except:
        pred_hw = [train["Quantity_Sold"].mean()] * test_horizon 
        
    results = pd.DataFrame({
        "Material_ID": [material_id] * test_horizon,
        "Sales_Date": test["Sales_Date"].values,
        "Actual_Qty": test["Quantity_Sold"].values,
        "Forecast_Prophet": pred_prophet,
        "Forecast_HoltWinters": pred_hw
    })
    return results

print("2. Exécution du Backtesting Massivement Parallèle (Spark)...")
df_algo_results = df_sales_ax.select("Material_ID", "Sales_Date", "Quantity_Sold").groupby("Material_ID").apply(run_backtesting_udf)

# =========================================================
# ÉTAPE B : INTÉGRATION HUMAIN & AUDIT TRAIL (BPF)
# =========================================================
print("3. Intégration des prévisions Humaines & Lead Times...")
df_evaluation = df_algo_results.join(
    df_forecast_ax, 
    on=["Material_ID", "Sales_Date"], 
    how="left"
)

# Ajout des Lead Times (Production/Achats) depuis la Master Data
df_evaluation = df_evaluation.join(
    df_mara_clean.select("Material_ID", "Mfg_LeadTime_Days", "Proc_LeadTime_Days"),
    on="Material_ID",
    how="left"
)

# =========================================================
# ÉTAPE C : LEAD TIME IMPACT & KPI STOCKOUT (EN DUR DANS SPARK)
# =========================================================
print("4. Calcul du MAPE, du Lead Time Impact et du Risque Rupture...")

def safe_error_pct(actual, forecast):
    return when(actual == 0, 0).otherwise((forecast - actual) / actual)

# 1. Calcul des Erreurs et Pourcentages d'Erreur
df_gold = df_evaluation \
    .withColumn("Forecast_Error_Pct_Prophet", safe_error_pct(col("Actual_Qty"), col("Forecast_Prophet"))) \
    .withColumn("Forecast_Error_Pct_HW", safe_error_pct(col("Actual_Qty"), col("Forecast_HoltWinters"))) \
    .withColumn("Forecast_Error_Pct_Planner", safe_error_pct(col("Actual_Qty"), col("Planner_Forecast_Qty")))

# 2. Calcul du Lead Time Impact (KPI Central PFE)
# Temps Total = Fabrication + Achats
df_gold = df_gold.withColumn("Total_Lead_Time_Days", col("Mfg_LeadTime_Days") + col("Proc_LeadTime_Days"))

# Impact en jours de stock (Abs(Error_Pct) * Total Lead Time)
df_gold = df_gold \
    .withColumn("Safety_Stock_Impact_Days_Prophet", abs(col("Forecast_Error_Pct_Prophet")) * col("Total_Lead_Time_Days")) \
    .withColumn("Safety_Stock_Impact_Days_Planner", abs(col("Forecast_Error_Pct_Planner")) * col("Total_Lead_Time_Days"))

# 3. Alertes et Flags (Stockout Risk)
# Si l'erreur est négative (Forecast < Actual) de plus de 20%, Risque Haut de Rupture (Stockout)
df_gold = df_gold \
    .withColumn("Stockout_Risk_Flag_Planner", 
                when(col("Forecast_Error_Pct_Planner") < -0.20, lit("HIGH"))
                .otherwise(lit("LOW"))) \
    .withColumn("Stockout_Risk_Flag_Prophet", 
                when(col("Forecast_Error_Pct_Prophet") < -0.20, lit("HIGH"))
                .otherwise(lit("LOW")))

print("5. Sauvegarde de la Base de Vérité vers Lakehouse Gold...")
# Conservation stricte : Predictions, Lead Times, Impact Métier, Audit Trail (BPF strict)
df_gold = df_gold.select(
    "Material_ID", "Sales_Date", 
    "Actual_Qty", "System_Forecast_Qty", "Planner_Forecast_Qty", "Forecast_Prophet", "Forecast_HoltWinters",
    "Mfg_LeadTime_Days", "Proc_LeadTime_Days", "Total_Lead_Time_Days",
    "Forecast_Error_Pct_Planner", "Safety_Stock_Impact_Days_Planner", "Stockout_Risk_Flag_Planner",
    "Forecast_Error_Pct_Prophet", "Safety_Stock_Impact_Days_Prophet", "Stockout_Risk_Flag_Prophet",
    "Modified_By_User_ID", "Modification_Timestamp", "Reason_Code" # BPF Audit
)

df_gold.write.format("delta").mode("overwrite").saveAsTable("SAP_GOLD_FORECAST_EVAL")

print("Moteur Gold, Impact Lead Time et Alertes BPF exécutés avec succès ! ✅")
