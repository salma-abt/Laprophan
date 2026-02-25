-- 03b_Gold_SQL_Validation.sql
-- Exécutez ce script SQL dans l'interface SQL Endpoint de Microsoft Fabric (Lakehouse Gold)
-- Objectif : Valider les données avant présentation aux métiers.

SELECT TOP 100
    Material_ID,
    Sales_Date,
    Actual_Qty,
    Planner_Forecast_Qty,
    Forecast_Prophet,
    -- 1. Métriques de Temps (Production/Achats)
    Mfg_LeadTime_Days,
    Proc_LeadTime_Days,
    Total_Lead_Time_Days,
    -- 2. Performance & Impact Planner vs Prophet
    ROUND(Forecast_Error_Pct_Planner * 100, 2) AS Error_Pct_Planner,
    ROUND(Forecast_Error_Pct_Prophet * 100, 2) AS Error_Pct_Prophet,
    ROUND(Safety_Stock_Impact_Days_Planner, 1) AS Impact_Days_Planner,
    ROUND(Safety_Stock_Impact_Days_Prophet, 1) AS Impact_Days_Prophet,
    -- 3. Alertes & Audit (BPF)
    Stockout_Risk_Flag_Planner,
    Reason_Code,
    Modified_By_User_ID,
    Modification_Timestamp
FROM SAP_GOLD_FORECAST_EVAL
WHERE Stockout_Risk_Flag_Planner = 'HIGH' -- Focus sur les alertes ruptures
ORDER BY Impact_Days_Planner DESC; -- Priorisation par impact supply chain le plus sévère
