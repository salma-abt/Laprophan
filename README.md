# FAAS4U - Plateforme IA Validation (Architecture Officielle CDC Laprophan)

Bienvenue dans l'implémentation de la plateforme **FAAS4U**, alignée stricto sensu sur le Cahier des Charges (Sections 3.1 et 4.2) de la Direction Supply Chain de Laprophan.

Ce dépôt Microsoft Fabric contient le socle **Medallion (Bronze/Silver/Gold)** pour l'évaluation et l'audit continu de vos **10 algorithmes de prévision internes** croisés avec la gouvernance BPF.

## 🚀 1. Couche BRONZE - Ingestion (CDC 3.1)
Le script `01_Generate_SAP_MockData.py` simule les 7 sources de données exigées :
1. **SAP MARA/MARC/MBEW** : Focus sur la segmentation ABC/XYZ et les **4 composantes du Lead Time** (Besoins MD61, Fabrication DZEIT, Libération BWKEY, Diffusion PLIFZ).
2. **SAP Ventes VBAK/VBAP** : L'historique des commandes clients.
3. **SAP Factures VBRK/VBRP** : L'historique des facturations (pour écart vs commandes).
4. **SAP Ruptures** : Les écarts constatés entre demande et disponibilité réelle.
5. **Prévisions IA (10 Algorithmes)** : Prophet, RN, ARIMAX, ETS, etc. sur horizon 1-24 mois.
6. **Audit Trail Planners** : La vérité humaine tracée de l'ERP (Override_Qty, User_ID, Reason_Code BPF).
7. **IQVIA** : Les ventes terrain, qui servent de Juge de Paix et Source de Vérité absolue pour le Backtesting.

## ⚙️ 2. Couche SILVER - Traitement (CDC 4.1)
Le script `02_Bronze_to_Silver.py` effectue 5 traitements globaux :
- **Nettoyage** des nulls et outliers.
- **Alignement Temporel** : Les 7 flux sont tronqués sur le grain "Mois" (Période S&OP).
- **Écarts Bruts** : Soustraction immédiate entre `Forecast_Algo` et `IQVIA_SellOut_Qty` (Vérité Terrain).
- **Segmentation Globale** : Conservation des 9 matrices (A/B/C x X/Y/Z). *Note : le filtrage segment AX n'est appliqué qu'en Phase 3 (BI/Dashboard) pour prouver le MVP, sans compromettre l'architecture.*

## 🧠 3. Couche GOLD - KPI & Observabilité (CDC 4.2)
Le script `03_Validation_Engine.py` génère les **3 Tables Officielles** en PySpark :
1. **`GOLD_PERFORMANCE_METRICS`** : Les 8 métriques absolues pré-calculées mathématiquement (MAPE, sMAPE, WAPE, MAE, RMSE, Biais, Theil's U, Tracking Signal).
2. **`GOLD_FVA_RESULTS`** : L'impact de l'humain. Le FVA (Forecast Value Added) vs le meilleur algorithme, l'Override Rate %, et la justification qualité (Reason_Code).
3. **`GOLD_ALERTES_DERIVE`** : La couche prescriptive. Déclenchement de la balise `DERIVE_DETECTEE` si Tracking Signal > 4, avec génération conditionnelle d'Alerte Rupture ou d'immobilisation financière (Lead Time Impact).

## 📊 4. KPI Power BI & DAX
Le fichier `04_DAX_Measures.dax` centralise l'activation dynamique des métriques précalculées de la Couche Gold pour afficher l'impact humain (Value Added) ou la criticité Supply Chain (Alerte Rupture) directement aux directeurs de Laprophan.
