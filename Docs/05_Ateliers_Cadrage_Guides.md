# 📅 Atelier de Cadrage — Plateforme FAAS4U (Phase Validation)

Ce document rassemble les invitations et guides pour mener à bien les ateliers de cadrage.
*Nouveau Contexte : FAAS4U est une plateforme de VALIDATION de prédictions fournies par un cabinet externe. L'enjeu est la mesure (KPI, Lead Time Impact, FVA, Audit), pas la création de l'IA.*

---

## 📑 1. Tableau de Suivi Global

| Direction | Contact Principal | Statut | Date Planifiée | Points Bloquants | Validé le |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Supply Chain** | [Nom] | À Planifier | | | |
| **Commercial** | [Nom] | À Planifier | | | |
| **Finance** | [Nom] | À Planifier | | | |
| **Production** | [Nom] | À Planifier | | | |
| **Achats** | [Nom] | À Planifier | | | |
| **IT / DNT** | [Nom] | Demande Envoyée| | Attente flux Cabinet/SAP | |
| **Qualité** | [Nom] | À Planifier | | | |

---

## 📧 2. Templates d'Emails d'Invitation (Par Direction)

### 📦 A. Supply Chain
**Sujet :** Atelier Cadrage PFE FAAS4U — Direction Supply Chain (Validation Cabinet)
**Bonjour [Nom],**
Mon projet PFE consiste à construire la plateforme FAAS4U sur Microsoft Fabric pour auditer, valider et mesurer la précision des prévisions fournies par notre cabinet externe. L'objectif de cet atelier est de valider vos critères d'acceptation de ces prévisions (ex: seuils MAPE/WAPE sur le segment AX) avant de les basculer dans SAP.
- **Sujets abordés :** Fiabilité du cabinet, cycle S&OP d'approbation, définition du segment prioritaire AX.
- **Disponibilités :** [Date 1], [Date 2] ou [Date 3].

### 🏭 B. Production & Achats (Session Groupée Optionnelle)
**Sujet :** Atelier Cadrage PFE FAAS4U — Impact Lead Time & Risque Rupture
**Bonjour [Noms],**
Dans le cadre de l'évaluation du cabinet de prévision, je modélise l'impact d'une erreur de leur part sur nos délais de fabrication (Production) et d'approvisionnement (Achats). FAAS4U calculera le "Lead Time Impact" pour sécuriser vos opérations.
- **Sujets abordés :** DZEIT (Fab), PLIFZ (Achats), lien direct entre un "mauvais forecast" du cabinet et un stock de sécurité menacé.
- **Disponibilités :** [Date 1], [Date 2], [Date 3].

### 🛡️ C. Qualité
**Sujet :** Atelier Cadrage PFE FAAS4U — Audit Trail BPF et Injection SAP
**Bonjour [Nom],**
Le projet FAAS4U centralisera les fichiers du cabinet, les données IQVIA et les corrections manuelles de nos Planners. L'atelier vise à s'assurer que notre plateforme Microsoft Fabric respecte la traçabilité GMP/BPF.
- **Sujets abordés :** Conservation des modifications humaines (FVA), Audit Trail avant injection dans SAP, règles d'anonymisation / Data Integrity.
- **Disponibilités :** [Date 1], [Date 2], [Date 3].

---

## 🎙️ 3. Guides d'Animation des Ateliers

**1. Introduction (5 min)**
- "Le PFE FAAS4U est notre 'Juge de Paix'. Il réceptionne les données du Cabinet IA, lit la réalité dans SAP (et le contexte IQVIA), et mesure objectivement qui a raison : le Cabinet ou notre Planner qui a corrigé le chiffre."

**2. Questions Métier (30 min) - Sélections prioritaires :**

*Supply Chain:*
- [ ] Q1 : Quel taux de précision attendez-vous du cabinet sur le segment AX pour qu'une prévision soit validée "auto" ?
- [ ] Q2 : Comment organisez-vous le "Feedback Loop" (remontée des erreurs) vers le cabinet ?

*Commercial & Finance:*
- [ ] Q1 : Le FVA (Forecast Value Added) vous aidera-t-il à justifier le ROI financier du prestataire externe ?
- [ ] Q2 : Comment les données de sortie IQVIA influencent-elles votre jugement sur la performance du cabinet ?

*IT / DNT:*
- [ ] Q1 : Comment s'opère le dépôt actuel des fichiers par le cabinet (Azure Blob, SFTP, Mails) ?
- [ ] Q2 : Quels sont les flux approuvés pour réinjecter "la bonne prévision" validée dans le plan de demande SAP ?

*Qualité:*
- [ ] Q1 : L'historisation du "Decision Log" (Le cabinet prévoyait X, le Planner a forcé Y) dans Fabric est-elle suffisante pour vos audits ?

---

## 📝 4. Template Collecte de Réponses ("Dossier Preuves")

| Direction | Question Posée | Réponse Obtenue (Cadrage) | Impact sur FAAS4U (Platform Design) | Preuve (Date) |
| :--- | :--- | :--- | :--- | :--- |
| Qualité | Validation Audit Trail | Requis: Timestamp exact + Matricule | Injection de fonctions Spark `current_timestamp()` dans la couche Gold avant écriture. | CR du 12/03 |
| IT | Fichiers Cabinet | Format CSV via SFTP | Nécessite un pipeline Data Factory `Copy Activity` vers Bronze Onelake. | CR du 14/03 |
| S. Chain | KPI tolérance | MAPE AX < 15% | Paramétrage condition DAX = Rouge si MAPE > 15% | CR du 15/03 |
