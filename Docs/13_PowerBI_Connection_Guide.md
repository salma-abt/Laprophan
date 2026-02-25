# =============================================================================
# GUIDE D'INTÉGRATION : DE MICROSOFT FABRIC À POWER BI DESKTOP
# Projet   : Plateforme FAAS4U (Microsoft Fabric)
# Auteure  : Salma | PFE Mundiapolis 2026
# Rôle     : Guide étape par étape pour l'import de données via DirectLake ou
#            SQL Endpoint depuis le Lakehouse de Fabric.
# =============================================================================

## Pré-requis
1. Avoir accès au workspace Microsoft Fabric "Laprophan FAAS4U".
2. Avoir téléchargé Power BI Desktop (dernière version).
3. Avoir uploadé les fichiers `.csv` dans *Files/* du Lakehouse Fabric (Voir `05_Load_CSV_to_OneLake.py` pour générer les Tables Delta).

---

## 🔗 ÉTAPE 1 : Connexion au point de terminaison SQL (SQL Endpoint) de Fabric
Microsoft Fabric connecte automatiquement vos tables Lakehouse à Power BI sans avoir à les télécharger (zéro copie - "DirectLake").

1. Dans **Microsoft Fabric**, ouvrez votre "Lakehouse" FAAS4U.
2. Tout en haut à droite, basculez le menu déroulant de `Lakehouse` à `Point de terminaison analytique SQL` (SQL Endpoint).
3. Cliquez sur le bouton "Paramètres" ⚙️ (engrenage en haut à droite) et trouvez votre **Chaîne de connexion SQL** (elle ressemble à `xxxxxxx.datawarehouse.pbidedicated.windows.net`).
4. **Copiez** ce lien.

---

## 🗃️ ÉTAPE 2 : Ouvrir et Connecter Power BI Desktop
1. Lancez **Power BI Desktop** sur votre machine.
2. Cliquez sur `Obtenir les données` (Get Data) -> `Plus...` (More...).
3. Recherchez **"Point de terminaison SQL Azure Synapse Analytics (DirectQuery)"** ou **"Azure SQL Database"**. (Vous pouvez aussi chercher "Microsoft Fabric" si disponible).
4. Collez la chaîne de connexion SQL récupérée à l'Étape 1 dans la case Serveur.
5. Choisissez le mode de connectivité des données : **DirectQuery**.
6. Connectez-vous avec votre compte institutionnel (celui utilisé pour Fabric).

---

## 📉 ÉTAPE 3 : Sélectionner les Tables Couche Gold
Une fois connecté, Power BI affichera toutes les bases de données du Lakehouse.

1. Développez l'arborescence et cochez ces 4 tables générées par le script PySpark FAAS4U :
   - ✅ `silver.forecasts_clean` (Pour les vues granulaires / Lead Times).
   - ✅ `gold.performance_metrics` (Pour le MAPE, Biais, Theil's U).
   - ✅ `gold.fva_results` (Pour la comparaison Algorithme vs. Humain).
   - ✅ `gold.alertes_derive` (Pour le Monitoring des modèles et alertes Tracking Signal).
2. Cliquez sur **Charger** (Load).

---

## 🧩 ÉTAPE 4 : Intégrer les Mesures DAX FAAS4U (Couche Sémantique)
Maintenant que les tables sont connectées :

1. Dans Power BI, allez dans l'onglet "Vue de modélisation" (Relations). Power BI aura peut-être détecté les clés `segment_abcxyz` entre les tables : vérifiez les relations (1 to Many).
2. Allez dans l'onglet **Rapport** (Report).
3. Ouvrez le fichier de code que nous venons de créer pour toi : `PowerBI/04_DAX_Measures.dax`.
4. Dans Power BI, clique droit sur la table `performance_metrics` -> **Nouvelle mesure** (New Measure).
5. Copie/colle la première section du code DAX (ex: `MAPE Moyen`). Répète pour l'ensemble du script DAX ou utilise des outils comme *Tabular Editor* pour insérer le code d'un seul coup.

---

## 📊 ÉTAPE 5 : Construire le Dashboard (Visuels Recommandés CDC)
Voici comment mapper tes données avec des visuels pour le COMEX Laprophan :

1. **Matrice / Heatmap (MAPE vs Cible ABC/XYZ) :**
   - *Lignes* : `segment_abc`
   - *Colonnes* : `segment_xyz`
   - *Valeurs* : `Mesure DAX [MAPE Moyen]`
   - *Critère formattage conditionnel* : Utilise la mesure DAX `[Couleur MAPE]`.
2. **Comparaison Humain vs Machine FVA (Cascade/Waterfall Chart) :**
   - *Catégorie* : `segment_abcxyz`
   - *Valeurs* : `mape_naive` ⬇️ `fva_algo_pts` ⬇️ `fva_humain_pts` -> Total `MAPE_DP`.
3. **Tableau des Dérives & Recommandations :**
   - Utiliser un visuel "Table" alimenté par `gold.alertes_derive` (`algorithme`, `segment`, `criticite`, `recommandation_claude`).

🎉 **Félicitations, ton dashboard est fonctionnel et connecté en temps réel via Fabric !**
N'oublie pas de Publish (Publier) ton rapport dans ton Workspace PFE Laprophan.
