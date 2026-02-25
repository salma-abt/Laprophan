# --- 01_Generate_SAP_MockData.py ---
# Rôle : Générer les 7 sources de données "Mock" exigées par le CDC (Section 3.1).
# Contexte Offciel : FAAS4U évalue 10 Algorithmes de prévision internes Laprophan sur un horizon de 1 à 24 mois,
# croisés avec des données complexes SAP (Ventes, Stocks, Ruptures) et IQVIA.

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession # type: ignore

# spark = SparkSession.builder.getOrCreate()

def generate_cdc_compliant_mock_data():
    np.random.seed(42)
    n_products = 100
    n_months_hist = 36 # 3 ans d'historique
    n_months_fcast = 24 # horizon 1-24 mois
    
    products = [f"PRD{str(i).zfill(4)}" for i in range(1, n_products + 1)]
    families = ['Antibiotiques', 'OTC', 'Maladies Chroniques']
    product_families = np.random.choice(families, size=n_products, p=[0.3, 0.4, 0.3])
    
    # -------------------------------------------------------------------------
    # SOURCE 1: SAP MASTER DATA & LEAD TIME (4 Composantes) - MARA / MARC / MBEW
    # -------------------------------------------------------------------------
    # Segmentation ABC/XYZ exhaustive (tous les segments)
    abc_classes = np.random.choice(['A', 'B', 'C'], size=n_products, p=[0.2, 0.3, 0.5])
    xyz_classes = np.random.choice(['X', 'Y', 'Z'], size=n_products, p=[0.2, 0.3, 0.5])
    
    # Lead Times (4 composantes du CDC)
    lt_besoins_md61 = np.random.randint(5, 15, size=n_products)
    lt_fab_dzeit = np.random.randint(15, 40, size=n_products)
    lt_qa_bwkey = np.random.randint(7, 21, size=n_products) # Libération Qualité
    lt_diff_plifz = np.random.randint(5, 20, size=n_products)
    
    df_mara_marc = pd.DataFrame({
        'Material_ID': products,
        'Product_Family': product_families,
        'ABC_Class': abc_classes,
        'XYZ_Class': xyz_classes,
        'LT_Besoins_Days': lt_besoins_md61,
        'LT_Fabrication_Days': lt_fab_dzeit,
        'LT_QA_Days': lt_qa_bwkey,
        'LT_Diffusion_Days': lt_diff_plifz,
        'Standard_Cost_MAD': np.random.uniform(50, 1500, size=n_products).round(2), # Source MBEW
        'Current_Stock_Qty': np.random.randint(0, 5000, size=n_products) # Source MBEW
    })

    # -------------------------------------------------------------------------
    # SOURCE 2 & 3: SAP VENTES (VBAK/VBAP) & FACTURES (VBRK/VBRP)
    # -------------------------------------------------------------------------
    dates_hist = pd.date_range(end=pd.Timestamp.today().replace(day=1), periods=n_months_hist, freq='MS')
    sales_data = []

    for idx, row in df_mara_marc.iterrows():
        base_demand = np.random.randint(1000, 5000) if row['ABC_Class'] == 'A' else np.random.randint(50, 800)
        noise_level = 0.05 if row['XYZ_Class'] == 'X' else 0.25
        seasonality = np.sin(np.arange(n_months_hist) * (2 * np.pi / 12)) * (base_demand * 0.3) if row['Product_Family'] == 'Antibiotiques' else 0
        
        demand = np.maximum(base_demand + seasonality + np.random.normal(0, base_demand * noise_level, n_months_hist), 0).round()
        
        for dt, dmd in zip(dates_hist, demand):
            sales_data.append({
                'Material_ID': row['Material_ID'],
                'Date': dt,
                'Order_Qty': dmd,                   # VBAK/VBAP
                'Invoiced_Qty': np.maximum(dmd - np.random.randint(0, int(dmd*0.1) + 1), 0), # VBRK/VBRP
            })
    df_sales = pd.DataFrame(sales_data)

    # -------------------------------------------------------------------------
    # SOURCE 4: HISTORIQUE DES RUPTURES (Écarts Demande / Dispo)
    # -------------------------------------------------------------------------
    df_sales['Shortage_Qty'] = df_sales['Order_Qty'] - df_sales['Invoiced_Qty']
    df_ruptures = df_sales[df_sales['Shortage_Qty'] > 0][['Material_ID', 'Date', 'Shortage_Qty']]

    # -------------------------------------------------------------------------
    # SOURCE 5: 10 ALGORITHMES DE PRÉVISION INTERNES (1 à 24 mois en arrière)
    # -------------------------------------------------------------------------
    algos = ['Neural_Network', 'Prophet', 'ARIMAX', 'Octopus_L', 'Holt_Winters', 'Auto_ARIMA', 'ETS', 'STLF', 'MAwS', 'TSLM']
    forecast_data = []
    
    for _, row in df_sales.iterrows():
        for algo in algos:
            algo_bias = np.random.uniform(0.05, 0.20)
            pred = np.maximum(row['Order_Qty'] + np.random.normal(0, row['Order_Qty'] * algo_bias), 0).round()
            forecast_data.append({
                'Material_ID': row['Material_ID'],
                'Target_Date': row['Date'],
                'Algorithm': algo,
                'Forecast_Qty': pred
            })
    df_10_algos = pd.DataFrame(forecast_data)

    # -------------------------------------------------------------------------
    # SOURCE 6: AUDIT TRAIL PLANNER (BPF) - Corrections Humaines
    # -------------------------------------------------------------------------
    df_planner = df_sales[['Material_ID', 'Date', 'Order_Qty']].copy()
    mask_modified = np.random.rand(len(df_planner)) < 0.25 # 25% d'Override Rate
    
    df_planner['Planner_Final_Qty'] = df_planner['Order_Qty']
    df_planner.loc[mask_modified, 'Planner_Final_Qty'] = np.maximum(df_planner.loc[mask_modified, 'Order_Qty'] * np.random.uniform(0.7, 1.3), 0).round()
    
    df_planner['Modified_By_User_ID'] = np.where(mask_modified, np.random.choice(['USER_001', 'USER_002', 'USER_003']), 'SYSTEM_AUTO')
    df_planner['Modification_Timestamp'] = np.where(mask_modified, pd.Timestamp.now() - pd.to_timedelta(np.random.randint(1, 30), unit='d'), pd.NaT)
    df_planner['Reason_Code'] = np.where(mask_modified, np.random.choice(['PROMO_EXPECTED', 'SUPPLIER_SHORTAGE', 'MARKET_SHIFT']), 'NO_CHANGE')
    df_planner = df_planner.rename(columns={'Date': 'Target_Date'})

    # -------------------------------------------------------------------------
    # SOURCE 7: IQVIA (Ventes Terrain - Source de vérité Backtesting sectoriel)
    # -------------------------------------------------------------------------
    df_iqvia = df_sales[['Material_ID', 'Date', 'Order_Qty']].copy()
    df_iqvia['IQVIA_SellOut_Qty'] = np.maximum(df_iqvia['Order_Qty'] + np.random.normal(0, df_iqvia['Order_Qty'] * 0.1), 0).round()
    df_iqvia = df_iqvia[['Material_ID', 'Date', 'IQVIA_SellOut_Qty']]

    return df_mara_marc, df_sales, df_ruptures, df_10_algos, df_planner, df_iqvia

print("1. Génération des 7 sources CDC...")
df_mara, df_sales, df_ruptures, df_10_algos, df_planner, df_iqvia = generate_cdc_compliant_mock_data()

print("2. Sauvegarde Bronze Layer (OneLake)...")
spark.createDataFrame(df_mara).write.format("delta").mode("overwrite").saveAsTable("BRONZE_SAP_MARA_MBEW")
spark.createDataFrame(df_sales).write.format("delta").mode("overwrite").saveAsTable("BRONZE_SAP_SALES_ORDERS")
spark.createDataFrame(df_ruptures).write.format("delta").mode("overwrite").saveAsTable("BRONZE_SAP_SHORTAGES")
spark.createDataFrame(df_10_algos).write.format("delta").mode("overwrite").saveAsTable("BRONZE_10_ALGOS_FORECASTS")
spark.createDataFrame(df_planner).write.format("delta").mode("overwrite").saveAsTable("BRONZE_PLANNER_AUDIT_BPF")
spark.createDataFrame(df_iqvia).write.format("delta").mode("overwrite").saveAsTable("BRONZE_IQVIA_SELLOUT")

print("Génération CDC Section 3.1 terminée ! ✅")
