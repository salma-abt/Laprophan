# --- 01_Generate_SAP_MockData.py ---
# Rôle : Générateur de Mock Data (Phase 1) pour peupler la couche Bronze (OneLake Fabric)
# Conformité : BPF/GMP (Audit Trail inclus), Segmentation ABC/XYZ, Prise en compte des Lead Times
# Cible d'exécution : Microsoft Fabric Synapse Notebook (PySpark)

import pandas as pd
import numpy as np
import datetime
from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F    # type: ignore

# Initialisation de la session Spark (Native dans MS Fabric)
# L'import permet d'éviter l'erreur d'analyseur de code local
try:
    spark = SparkSession.builder.getOrCreate()
except:
    pass

# ==========================================
# 0. CONFIGURATION & PARAMÈTRES (Audit BPF)
# ==========================================
RUN_DATE = datetime.datetime.utcnow()
AUDIT_USER = "Salma (Fabric Sync)"
REASON = "Initialisation Phase Cadrage (Mock Data)"

print(f"🚀 Début de l'ingestion Mock Data - {RUN_DATE}")

def add_audit_trail(df_spark):
    """Ajoute les champs d'audit obligatoires BPF/GMP à n'importe quel DataFrame PySpark"""
    return df_spark \
        .withColumn("Sys_LoadDate", F.lit(RUN_DATE)) \
        .withColumn("Sys_LoadedBy", F.lit(AUDIT_USER)) \
        .withColumn("Sys_Reason", F.lit(REASON))

# Paramètres de simulation
np.random.seed(42)  # Reproductibilité
N_SKUS = 100
MONTHS_HIST = 24

# ==========================================
# 1. GÉNÉRATION DES DONNÉES (Via Pandas pour la vélocité puis Spark)
# ==========================================

# A. MASTER DATA (DCI, ATC, ABC/XYZ)
skus = [f"LAP-{str(i).zfill(4)}" for i in range(1, N_SKUS + 1)]
abc_classes = np.random.choice(['A', 'B', 'C'], size=N_SKUS, p=[0.2, 0.3, 0.5])
xyz_classes = np.random.choice(['X', 'Y', 'Z'], size=N_SKUS, p=[0.2, 0.3, 0.5])
atc_classes = np.random.choice(['A (Digestif)', 'C (Cardio)', 'J (Anti-infectieux)', 'N (Nerveux)'], size=N_SKUS)

pdf_master = pd.DataFrame({
    'SKU_ID': skus,
    'Description': [f"Medicament_{sku}" for sku in skus],
    'DCI': [f"PrincipeActif_{np.random.randint(1,20)}" for _ in skus],
    'ATC_Class': atc_classes,
    'Segment_ABC': abc_classes,
    'Segment_XYZ': xyz_classes
})

# B. LEAD TIMES (Fabrication & Approvisionnement)
pdf_lead_times = pd.DataFrame({
    'SKU_ID': skus,
    'Appro_MP_Jours': np.random.randint(15, 60, size=N_SKUS), # Délais MP importées
    'Fabrication_Jours': np.random.randint(5, 20, size=N_SKUS),
    'Controle_Qualite_Jours': np.random.randint(7, 14, size=N_SKUS) # Critique en pharma
})

# C. SAP SALES HISTORY (24 mois + Bruit selon ABC/XYZ)
dates_hist = pd.date_range(end=pd.Timestamp.today().replace(day=1), periods=MONTHS_HIST, freq='MS')
sales_records = []

for idx, row in pdf_master.iterrows():
    # AX sont stables et gros volumes (volatilité faible)
    base_qty = np.random.randint(5000, 15000) if row['Segment_ABC'] == 'A' else np.random.randint(100, 1000)
    volatility = 0.05 if row['Segment_XYZ'] == 'X' else 0.30
    
    # Génération d'un historique avec un peu de saisonnalité
    qty_series = np.maximum(np.random.normal(base_qty, base_qty * volatility, MONTHS_HIST), 0).astype(int)
    
    for dt, qty in zip(dates_hist, qty_series):
        sales_records.append({
            'Transaction_ID': f"TRX_{row['SKU_ID']}_{dt.strftime('%Y%m')}",
            'SKU_ID': row['SKU_ID'],
            'Date': dt.date(),
            'Quantity_Sold': float(qty),
            'Client_Type': np.random.choice(['Grossiste', 'Hopital', 'Export'])
        })

pdf_sales = pd.DataFrame(sales_records)

# D. FORECAST OUTPUT FORMAT (Table de l'Annexe B de vos docs)
# Mots-clés des modèles: Prophet, NeuralNet, Human_Expert
forecast_records = []
for sku in skus:
    for model in ['Prophet', 'NeuralNet', 'Human_Expert']:
        for horizon, h_label in zip([1, 4, 12], ['1-3mo', '3-6mo', '6-24mo']):
            Target_date = pd.Timestamp.today() + pd.DateOffset(months=horizon)
            base_f = np.random.randint(500, 5000)
            forecast_records.append({
                'SKU_ID': sku,
                'Periode': Target_date.date(),
                'History': float(base_f * 0.9), # Historique fictif
                'Corrected': float(base_f * 0.95), # Corrigé des outliers
                'Forecast': float(base_f),
                'IC_Bas': float(base_f * 0.8),
                'IC_Haut': float(base_f * 1.2),
                'ModelUsed': model,
                'Forecast_Horizon': h_label
            })

pdf_forecasts = pd.DataFrame(forecast_records)

# ==========================================
# 2. SAUVEGARDE EN FORMAT DELTA DANS ONELAKE (Architecture Bronze)
# ==========================================
print("🔄 Conversion en Spark DataFrames et application de l'Audit BPF...")

# Fonction utilitaire pour écrire en Delta (Mode écrasement pour le Mock)
def save_to_bronze(pdf, table_name):
    # Conversion Pandas -> Spark
    df_spark = spark.createDataFrame(pdf)
    # Ajout de la traçabilité GMP
    df_spark_audit = add_audit_trail(df_spark)
    # Sauvegarde native OneLake (Delta) - Le format requis
    df_spark_audit.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"✅ Table sauvegardée : {table_name} (Lignes : {df_spark.count()})")

# Exécution de l'écriture
save_to_bronze(pdf_master, "bronze_master_data")
save_to_bronze(pdf_lead_times, "bronze_lead_times")
save_to_bronze(pdf_sales, "bronze_sap_sales")
save_to_bronze(pdf_forecasts, "bronze_forecast_output")

print("🎉 PHASE 1 (Couche Bronze) INITIALISÉE AVEC SUCCÈS SUR MICROSOFT FABRIC.")
