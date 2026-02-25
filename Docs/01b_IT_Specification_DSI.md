# 📋 Spécification Technique - Extraction SAP, IQVIA & Cabinet (Projet FAAS4U)

Ce document formalise les besoins en données pour la DSI Laprophan, dans le cadre de la plateforme de validation **FAAS4U**.
*Objectif du PFE : Ingestion, backtesting et validation BPF des prévisions fournies par le cabinet externe, avant injection dans SAP S/4HANA.*

## 1. Tableaux des Données Requises (Sources Multiples)

### A. Flux SAP S/4HANA (Master Data & Ventes Réelles)
**Objectif :** Obtenir les ventes réelles pour le backtesting (calcul d'erreur) et les Lead Times pour mesurer l'impact métier.
*Fréquence d'extraction : Quotidienne (Delta).*

| Champ fonctionnel | Table SAP | Champ SAP | Description (Métier) | 
| :--- | :--- | :--- | :--- | 
| **Material ID** | `MARA` / `MARC` | `MATNR` | Code Article (FERT) - Filtrage MVP Segment AX | 
| **Plant ID** | `MARC` | `WERKS` | Division (Ex: LAP1) | 
| **Prod. Lead Time** | `MARC` | `DZEIT` | Délai de fabrication (jours) - Clé pour impact KPI | 
| **Proc. Lead Time** | `MARC` | `PLIFZ` | Délai de livraison (jours) - Clé pour impact KPI | 
| **Standard Price** | `MBEW` | `STPRS` | Coût standard (MAD) | 
| **Sales Date & Qty** | `VBAK` / `VBAP` | `AUDAT` / `KWMENG` | Historique des ventes pour comparaison (Backtesting) | 
| **Planner Forecast** | `PBIM` / `PBED` | `PLNMG` / `Z_MODQTY`| Prévisions corrigées manuellement par les planners |
| **Audit Trail BPF** | `CDHDR` / `CDPOS`| `USERNAME` / `UDATE`| **CRITIQUE :** Traceabilité des interventions humaines |

### B. Flux Cabinet Externe (Les Prévisions IA)
**Objectif :** Récupérer la sortie brute de l'algorithme du cabinet pour évaluation de la fiabilité.
*Intégration attendue : SFTP, API REST, ou Azure Blob Storage natif vers Fabric.*

| Champ fonctionnel | Fichier Cabinet | Description |
| :--- | :--- | :--- |
| **Material ID** | `SKU_Code` | Doit être mappable avec MATNR de SAP |
| **Target Date** | `Forecast_Month` | Horizon de la prévision (ex: Mois M+1 à M+24) |
| **Cabinet Forecast**| `Predicted_Qty` | La quantité prédite calculée par leur IA |
| **Confidence Score**| `Confidence_Pct` | (Optionnel) Intervalle de confiance fourni par le cabinet |

### C. Flux IQVIA (Données Marché Pharma)
**Objectif :** Contexte externe pour expliquer les dérives (ex: baisse de part de marché globale signalée par IQVIA vs erreur algorithmique du cabinet).
*Intégration attendue : Fichiers plats mensuels ou API IQVIA.*

---

## 2. Modèle d'e-mail final pour l'IT / DSI Laprophan

**Sujet :** Demande de flux Data (Cabinet, SAP, IQVIA) - Validation PFE FAAS4U - Urgence Académique

**À :** [Nom du Responsable DNT / Architecte SAP]
**Cc :** [Nom du Sponsor Supply Chain Laprophan]

Bonjour [Nom du responsable DNT],

Dans le cadre du projet PFE **FAAS4U**, je suis chargée de construire la plateforme de **validation de fiabilité et de performance** des prévisions qui nous sont fournies par notre cabinet partenaire. 
Cette plateforme, construite sur **Microsoft Fabric**, a pour but de comparer la performance des modèles du cabinet avec les corrections humaines, d'en évaluer l'impact sur nos Lead Times de sécurité, et de garantir un Audit Trail conforme aux normes BPF pour toute injection finale dans SAP S/4HANA (Planned Independent Requirements).

Afin d'avancer sur mon MVP centré sur les articles du **Segment AX** (et compte tenu de l'urgence académique de ma soutenance approchante), j'aurais besoin d'étudier avec vous les modalités techniques d'ingestion pour les trois flux suivants vers notre Lakehouse OneLake :

1. **Ingestion des fichiers du Cabinet** : Quel est le protocole actuel de réception de leurs prévisions (SFTP, API, dépôt manuel) ? Pouvons-nous automatiser un flux Data Factory copy vers Fabric ?
2. **Extraction SAP S/4HANA** : Faisabilité d'extraire l'historique des ventes (`VBAK`/`VBAP`), la Master Data (`MARC` - Lead Times) et surtout **l'Audit Trail BPF** (`CDHDR`/`CDPOS`) pour tracer les modifications manuelles des Demand Planners.
3. **Données IQVIA** : Modalités d'accès aux rapports mensuels du marché pour intégration de données contextuelles externes.

*Concernant la Qualité (BPF) et le RGPD :* Le but n'est pas de pister les utilisateurs SAP, mais de différencier techniquement "l'algorithme du cabinet" de "l'action du planner" pour calculer le Forecast Value Added (FVA). Si souhaité, l'identifiant SAP peut être pseudonymisé.

Seriez-vous disponible **[Proposer Créneau 1]**, **[Proposer Créneau 2]**, ou **[Proposer Créneau 3]** pour un point technique de 30 minutes visant à valider cette architecture d'ingestion ?

Merci par avance pour votre précieux accompagnement.

Bien cordialement,

**Salma [Nom de Famille]**
Élève Ingénieure Data & IA - PFE Supply Chain
Laprophan
