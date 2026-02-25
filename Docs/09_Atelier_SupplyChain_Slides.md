# =============================================================================
# ATELIER SUPPLY CHAIN : PRÉSENTATION BACKTESTING FAAS4U
# Projet   : Plateforme FAAS4U (Microsoft Fabric)
# Date     : [A COMPLETER]
# Audience : Direction Supply Chain, Demand Planners, IT
# Auteure  : Salma | PFE Mundiapolis 2026
# =============================================================================

---

## 🎯 SLIDE 1: Introduction & Objectifs de l'Atelier
**Titre : Restitution du Backtesting FAAS4U (Phase 1)**

**Objectifs de la session (1h) :**
1. Présenter la méthode de validation des prévisions (Architecture Medallion Fabric).
2. Analyser les premiers résultats de Backtesting sur les données historiques SAP/IQVIA.
3. Quantifier l'impact des corrections manuelles (FVA - Forecast Value Added).
4. Valider collectivement les KPI cibles par segment (ABC/XYZ) pour la mise en production.

---

## 🚀 SLIDE 2: Contexte FAAS4U & Enjeux Métiers
**Rappel du Problème (cf. Présentation COMEX) :**
- Complexité croissante (250+ SKUs), lead times incompressibles (importations MP).
- Impact direct des erreurs prévisionnelles : Surstocks (immobilisation financière) et Ruptures (perte de CA).

**La Solution FAAS4U :**
- Ce n'est **pas** un nouvel algorithme de prévision.
- C'est un **Moteur d'Évaluation** Microsoft Fabric (Bronze/Silver/Gold) validant :
  - Quel algo performe le mieux par segment produit ?
  - Quand et comment l'humain (Demand Planner) doit-il intervenir ?
- **Objectif MVP :** Améliorer la précision (MAPE) pour réduire le BFR. ROI attendu < 12 mois.

---

## 📊 SLIDE 3: Premiers Résultats du Backtesting (Algorithmes)
*Basé sur les métriques calculées en Couche Gold (10 algos évalués).*

**Meilleurs algorithmes par segment stratégique (Horizon 1 mois) :**
| Segment | Profil Métier | Meilleur Algorithme | MAPE Démontré | Performance vs Naïf (Theil's U) |
| :--- | :--- | :--- | :--- | :--- |
| **AX** | Fort CA / Stable | Octopus_L | ~9.5% | Score 0.31 (Excellente amélioration vs baseline) |
| **AY** | Fort CA / Modéré | Neural_Network | ~9.6% | Score 0.12 (Ajustement quasi-parfait) |
| **BX** | CA Moyen / Stable | ARIMAX / Prophet | ~10.7 - 11.6% | Score < 0.33 (Forte robustesse) |

*💡 Insight : Sur les produits très prédictibles (AX/AY), les modèles complexes surpassent largement la moyenne historique.*

---

## 👤 SLIDE 4: Le "Forecast Value Added" (FVA)
*Où se situe la véritable valeur ajoutée métier ? (cf. Section 4.2 CDC)*

**Analyse de l'Humain vs Machine :**
- **Processus Évalué :** Prévision Naïve ➡️ Modèle IA ➡️ Correction Demand Planner.
- **Résultats initiaux sur l'échantillon de test :**
  - Dans la majorité des cas ciblés (AX/AY), les algorithmes *meilleurs certifiés* atteignent les cibles MAPE.
  - Dans de nombreuses situations observées, l'intervention manuelle a été flaggée comme **HUMAIN_DEGRADE_SIGNIFICATIVEMENT**.
- **Objectif :** Ne pas supprimer l'expert, mais *concentrer son temps* sur les segments Z (haute volatilité, promotions, ruptures) où la machine manque de contexte.

---

## 🛑 SLIDE 5: Points d'Arbitrage (Extraction Questions PFE)
*Questions issues de notre document de cadrage ("Questions_Cadrage_Laprophan_FAAS4U") :*

1. **Validation des Cibles MAPE :** Êtes-vous d'accord pour fixer les seuils de tolérance à :
   - Segment A : MAPE < 10%
   - Segment B : MAPE < 20%
   - Segment C : MAPE < 40% ?
2. **Alertes Tracking Signal :** Le seuil standard de ±6 a été configuré. Si un produit cumule 6 périodes de "sous-prévisions", FAAS4U envoie une alerte. Ce délai est-il adapté à vos Lead Times de fabrication ?
3. **Vérité Terrain :** Confirmez-vous que les ventes IQVIA doivent rester la cible "Gold" (Actuals) plutôt que les livraisons SAP (polluées par les ruptures) ?

---

## 🔭 SLIDE 6: Prochaines Étapes & Décisions
**Décisions attendues à l'issue de cet atelier :**
- [ ] GO/NO-GO sur la règle de segmentation (IQVIA 12 mois glissants).
- [ ] GO/NO-GO sur les cibles MAPE par lettre (A/B/C).
- [ ] Alignement sur l'usage du Tableau de Bord Power BI (Vue FVA & Heatmap).

**Prochaine Phase (Semaine Prochaine) :**
- Restitution du Dashboard Power BI avec navigation dynamique par Segment.
- Mapping des flux de retour (Comment réinjecter les KPI dans le processus S&OP mensuel).

---
*Fin du document préparatoire.*
