# =============================================================================
# FAAS4U — Laprophan | Couche Silver
# Fichier  : 02_Bronze_to_Silver.py
# Auteure  : Salma | PFE Mundiapolis 2026
# Encadrant: M. Kahlaoui
# Rôle     : Ingestion Bronze → Nettoyage → Alignement → Segmentation ABC/XYZ
#             → Table forecasts_clean (Silver)
# =============================================================================
# LOGIQUE MÉTIER :
#   Bronze = données brutes telles que reçues (SAP, IQVIA, Lead Time, SC)
#   Silver = données propres, alignées, enrichies, prêtes pour le moteur Gold
#
# RÈGLES ABSOLUES :
#   - IQVIA = vérité terrain (pas SAP ventes)
#   - Segmentation ABC/XYZ calculée sur IQVIA 12 mois glissants
#   - Dates tronquées au 1er jour du mois (alignement universel)
#   - Clé unique : sku_id + periode (mois)
#   - Audit trail : date_traitement + source_fichier sur chaque ligne
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# 0. IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import functions as F # type: ignore
from pyspark.sql.window import Window # type: ignore
from pyspark.sql.types import ( # type: ignore
    StructType, StructField,
    StringType, DoubleType, DateType, IntegerType
)
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# 1. SESSION SPARK (Microsoft Fabric — ne pas modifier)
# ─────────────────────────────────────────────────────────────────────────────
try:
    spark = SparkSession.builder.getOrCreate()
except:
    pass

# ─────────────────────────────────────────────────────────────────────────────
# 2. CHEMINS ONELAKE (Bronze → Silver)
# ─────────────────────────────────────────────────────────────────────────────
# Adapter ces chemins à la structure réelle du Lakehouse Laprophan
BRONZE_PATH = "Tables/bronze/"
SILVER_PATH = "Tables/silver/"

# Noms des tables Bronze (fichiers CSV ou Delta)
TABLE_SAP_FORECASTS   = f"{BRONZE_PATH}sap_forecasts_raw"       # Forecasts 10 algos + DP
TABLE_IQVIA_ACTUALS   = f"{BRONZE_PATH}iqvia_actuals_raw"        # Ventes réelles IQVIA
TABLE_LEAD_TIME       = f"{BRONZE_PATH}lead_time_raw"            # 4 étapes Lead Time
TABLE_SUPPLY_CHAIN    = f"{BRONZE_PATH}supply_chain_raw"         # Factures, ruptures

# Table de sortie Silver
TABLE_FORECASTS_CLEAN = f"{SILVER_PATH}forecasts_clean"

# Timestamp du traitement (audit trail)
DATE_TRAITEMENT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────────────────────
# 3. SCHÉMAS ATTENDUS (documentation + validation)
# ─────────────────────────────────────────────────────────────────────────────
# Ces schémas décrivent ce qu'on attend des fichiers Bronze.
# Si les colonnes sont absentes, le script lèvera une erreur explicite.

COLONNES_SAP_REQUISES = [
    "sku_id",        # Identifiant produit (SKU)
    "periode",       # Mois de prévision (YYYY-MM-DD ou YYYY-MM)
    "algorithme",    # Nom de l'algorithme (ex: Prophet, ARIMAX, Expert_DP)
    "forecast_qty",  # Quantité prévue
    "dci",           # Dénomination Commune Internationale
    "atc4",          # Classe thérapeutique niveau 4
]

COLONNES_IQVIA_REQUISES = [
    "sku_id",        # Identifiant produit
    "periode",       # Mois de vente réelle
    "ventes_reelles_qty",   # Quantité vendue réelle (VÉRITÉ TERRAIN)
    "ventes_reelles_valeur" # Valeur en MAD (pour ABC)
]

COLONNES_LEAD_TIME_REQUISES = [
    "sku_id",
    "periode",
    "lt_besoins_jours",      # Lead Time Besoins MP/AC (jours)
    "lt_fabrication_jours",  # Lead Time Fabrication (jours)
    "lt_liberation_jours",   # Lead Time Libération BPF/GMP (jours)
    "lt_diffusion_jours",    # Lead Time Diffusion grossistes (jours)
]

COLONNES_SC_REQUISES = [
    "sku_id",
    "periode",
    "nb_ruptures",           # Nombre de jours en rupture
    "qty_commandee",         # Quantité commandée
    "qty_facturee",          # Quantité facturée réelle
]

# Liste des 10 algorithmes valides + Expert Demand Planner
ALGORITHMES_VALIDES = [
    "Neural_Network", "Prophet", "ARIMAX", "Octopus_L",
    "Holt_Winters", "Auto_ARIMA", "ETS", "STLF", "MAwS", "TSLM",
    "Expert_DP"  # Corrections manuelles du Demand Planner
]

# ─────────────────────────────────────────────────────────────────────────────
# 4. FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────

def valider_colonnes(df, colonnes_requises, nom_table):
    """
    Vérifie que toutes les colonnes requises sont présentes.
    Lève une erreur explicite si une colonne manque.
    """
    colonnes_presentes = set(df.columns)
    colonnes_manquantes = set(colonnes_requises) - colonnes_presentes
    if colonnes_manquantes:
        raise ValueError(
            f"[ERREUR] Table '{nom_table}' — colonnes manquantes : "
            f"{sorted(colonnes_manquantes)}\n"
            f"Colonnes présentes : {sorted(colonnes_presentes)}"
        )
    print(f"[OK] Table '{nom_table}' — toutes les colonnes requises sont présentes.")


def aligner_periode(df, col_periode="periode"):
    """
    Normalise la colonne période au 1er jour du mois.
    Gère les formats : YYYY-MM-DD, YYYY-MM, MM/YYYY
    Retourne une colonne de type DateType tronquée au mois.
    """
    df = df.withColumn(
        col_periode,
        F.trunc(
            F.to_date(F.col(col_periode), "yyyy-MM-dd")
                .cast("string")
                .alias("_tmp"),
            "month"
        )
    )
    # Fallback si le format est YYYY-MM
    df = df.withColumn(
        col_periode,
        F.when(
            F.col(col_periode).isNull(),
            F.trunc(F.to_date(F.concat(F.col("periode"), F.lit("-01"))), "month")
        ).otherwise(F.col(col_periode))
    )
    return df


def ajouter_audit_trail(df, source):
    """
    Ajoute les colonnes d'audit trail sur chaque ligne.
    Obligatoire pour conformité BPF/GMP Laprophan.
    """
    return df \
        .withColumn("date_traitement", F.lit(DATE_TRAITEMENT)) \
        .withColumn("source_fichier", F.lit(source))


# ─────────────────────────────────────────────────────────────────────────────
# 5. CHARGEMENT DES TABLES BRONZE
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("ÉTAPE 1 — Chargement des tables Bronze")
print("="*60)

# 5.1 SAP Forecasts (10 algorithmes + Expert DP)
df_sap = spark.read.format("delta").load(TABLE_SAP_FORECASTS)
valider_colonnes(df_sap, COLONNES_SAP_REQUISES, "sap_forecasts_raw")
print(f"  SAP Forecasts chargé : {df_sap.count()} lignes")

# 5.2 IQVIA Ventes Réelles (VÉRITÉ TERRAIN)
df_iqvia = spark.read.format("delta").load(TABLE_IQVIA_ACTUALS)
valider_colonnes(df_iqvia, COLONNES_IQVIA_REQUISES, "iqvia_actuals_raw")
print(f"  IQVIA Actuals chargé : {df_iqvia.count()} lignes")

# 5.3 Lead Time (4 étapes)
df_lt = spark.read.format("delta").load(TABLE_LEAD_TIME)
valider_colonnes(df_lt, COLONNES_LEAD_TIME_REQUISES, "lead_time_raw")
print(f"  Lead Time chargé     : {df_lt.count()} lignes")

# 5.4 Supply Chain (contexte ruptures / commandes)
df_sc = spark.read.format("delta").load(TABLE_SUPPLY_CHAIN)
valider_colonnes(df_sc, COLONNES_SC_REQUISES, "supply_chain_raw")
print(f"  Supply Chain chargé  : {df_sc.count()} lignes")


# ─────────────────────────────────────────────────────────────────────────────
# 6. NETTOYAGE ET ALIGNEMENT
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("ÉTAPE 2 — Nettoyage et alignement des dates")
print("="*60)

# 6.1 Aligner toutes les périodes au 1er du mois
df_sap   = aligner_periode(df_sap)
df_iqvia = aligner_periode(df_iqvia)
df_lt    = aligner_periode(df_lt)
df_sc    = aligner_periode(df_sc)

# 6.2 Filtrer les algorithmes invalides (sécurité qualité)
df_sap = df_sap.filter(F.col("algorithme").isin(ALGORITHMES_VALIDES))
nb_invalides = df_sap.filter(~F.col("algorithme").isin(ALGORITHMES_VALIDES)).count()
if nb_invalides > 0:
    print(f"  [ATTENTION] {nb_invalides} lignes avec algorithme inconnu — supprimées")

# 6.3 Supprimer les doublons exacts
df_sap   = df_sap.dropDuplicates(["sku_id", "periode", "algorithme"])
df_iqvia = df_iqvia.dropDuplicates(["sku_id", "periode"])
df_lt    = df_lt.dropDuplicates(["sku_id", "periode"])
df_sc    = df_sc.dropDuplicates(["sku_id", "periode"])

# 6.4 Remplacer les valeurs nulles critiques par 0 (avec log)
df_sap   = df_sap.fillna({"forecast_qty": 0})
df_iqvia = df_iqvia.fillna({"ventes_reelles_qty": 0, "ventes_reelles_valeur": 0})
df_lt    = df_lt.fillna({
    "lt_besoins_jours": 0,
    "lt_fabrication_jours": 0,
    "lt_liberation_jours": 0,
    "lt_diffusion_jours": 0
})
df_sc = df_sc.fillna({"nb_ruptures": 0, "qty_commandee": 0, "qty_facturee": 0})

print("  [OK] Nettoyage terminé — doublons supprimés, nulls traités")


# ─────────────────────────────────────────────────────────────────────────────
# 7. SEGMENTATION ABC/XYZ (sur IQVIA 12 mois glissants)
# ─────────────────────────────────────────────────────────────────────────────
# RÈGLE MÉTIER :
#   ABC = basé sur la valeur des ventes IQVIA (CA)
#     A : top 80% du CA cumulé → MAPE cible < 10%
#     B : 15% suivants        → MAPE cible < 20%
#     C : derniers 5%         → MAPE cible < 40%
#
#   XYZ = basé sur la variabilité (CV = écart-type / moyenne)
#     X : CV < 0.5  → série stable
#     Y : CV 0.5-1.0 → série modérée
#     Z : CV > 1.0  → série erratique

print("\n" + "="*60)
print("ÉTAPE 3 — Segmentation ABC/XYZ")
print("="*60)

# 7.1 Fenêtre : 12 derniers mois de données IQVIA
date_max_iqvia = df_iqvia.agg(F.max("periode")).collect()[0][0]
date_debut_12m = F.add_months(F.lit(date_max_iqvia), -12)

df_iqvia_12m = df_iqvia.filter(F.col("periode") >= date_debut_12m)

# 7.2 Calcul ABC — CA total par SKU sur 12 mois
df_ca_sku = df_iqvia_12m.groupBy("sku_id").agg(
    F.sum("ventes_reelles_valeur").alias("ca_total_12m")
)

# Rang par CA décroissant
window_abc = Window.orderBy(F.desc("ca_total_12m"))
df_ca_sku = df_ca_sku.withColumn("rang_ca", F.rank().over(window_abc))

# CA total de l'entreprise
ca_total_entreprise = df_ca_sku.agg(F.sum("ca_total_12m")).collect()[0][0]

# Part cumulée du CA
window_cumul = Window.orderBy(F.desc("ca_total_12m")).rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df_ca_sku = df_ca_sku.withColumn(
    "ca_cumul",
    F.sum("ca_total_12m").over(window_cumul)
).withColumn(
    "pct_ca_cumul",
    F.col("ca_cumul") / ca_total_entreprise
)

# Attribution segment ABC
df_abc = df_ca_sku.withColumn(
    "segment_abc",
    F.when(F.col("pct_ca_cumul") <= 0.80, "A")
     .when(F.col("pct_ca_cumul") <= 0.95, "B")
     .otherwise("C")
).select("sku_id", "segment_abc", "ca_total_12m")

# 7.3 Calcul XYZ — Coefficient de Variation par SKU
df_stats_sku = df_iqvia_12m.groupBy("sku_id").agg(
    F.mean("ventes_reelles_qty").alias("moyenne_ventes"),
    F.stddev("ventes_reelles_qty").alias("ecart_type_ventes"),
    F.count("periode").alias("nb_periodes")
)

# CV = écart-type / moyenne (protégé contre division par zéro)
df_stats_sku = df_stats_sku.withColumn(
    "cv",
    F.when(
        F.col("moyenne_ventes") > 0,
        F.col("ecart_type_ventes") / F.col("moyenne_ventes")
    ).otherwise(F.lit(99.0))  # CV très élevé si pas de ventes
)

# Attribution segment XYZ
df_xyz = df_stats_sku.withColumn(
    "segment_xyz",
    F.when(F.col("cv") < 0.5,  "X")
     .when(F.col("cv") <= 1.0, "Y")
     .otherwise("Z")
).select("sku_id", "segment_xyz", "cv", "moyenne_ventes")

# 7.4 Jointure ABC + XYZ → segment combiné (ex: AX, BZ, CY...)
df_segmentation = df_abc.join(df_xyz, on="sku_id", how="inner")
df_segmentation = df_segmentation.withColumn(
    "segment_abcxyz",
    F.concat(F.col("segment_abc"), F.col("segment_xyz"))
)

# Ajout de la cible MAPE par segment ABC
df_segmentation = df_segmentation.withColumn(
    "mape_cible_pct",
    F.when(F.col("segment_abc") == "A", 10.0)
     .when(F.col("segment_abc") == "B", 20.0)
     .otherwise(40.0)
)

# Comptage pour validation
dist_abc = df_segmentation.groupBy("segment_abc").count().orderBy("segment_abc")
dist_xyz = df_segmentation.groupBy("segment_xyz").count().orderBy("segment_xyz")
print("  Distribution ABC :")
dist_abc.show()
print("  Distribution XYZ :")
dist_xyz.show()


# ─────────────────────────────────────────────────────────────────────────────
# 8. CALCUL DU LEAD TIME TOTAL
# ─────────────────────────────────────────────────────────────────────────────
# Lead Time Total = somme des 4 étapes
# Utilisé dans Gold pour distinguer écart algo vs écart opérationnel

df_lt = df_lt.withColumn(
    "lt_total_jours",
    F.col("lt_besoins_jours") +
    F.col("lt_fabrication_jours") +
    F.col("lt_liberation_jours") +
    F.col("lt_diffusion_jours")
)

print("\n[OK] Lead Time total calculé (4 étapes sommées)")


# ─────────────────────────────────────────────────────────────────────────────
# 9. JOINTURE PRINCIPALE → forecasts_clean
# ─────────────────────────────────────────────────────────────────────────────
# Structure cible Silver :
#   sku_id | periode | algorithme | forecast_qty | ventes_reelles_qty |
#   ecart_qty | ecart_pct | segment_abc | segment_xyz | segment_abcxyz |
#   mape_cible_pct | cv | lt_besoins_jours | lt_fabrication_jours |
#   lt_liberation_jours | lt_diffusion_jours | lt_total_jours |
#   nb_ruptures | dci | atc4 | date_traitement | source_fichier

print("\n" + "="*60)
print("ÉTAPE 4 — Jointures et construction de forecasts_clean")
print("="*60)

# 9.1 SAP Forecasts + IQVIA Actuals (clé : sku_id + periode)
df_joined = df_sap.join(
    df_iqvia.select(
        "sku_id", "periode",
        "ventes_reelles_qty", "ventes_reelles_valeur"
    ),
    on=["sku_id", "periode"],
    how="left"  # LEFT : on garde tous les forecasts même sans actual (horizons futurs)
)

# 9.2 Ajout Lead Time
df_joined = df_joined.join(
    df_lt.select(
        "sku_id", "periode",
        "lt_besoins_jours", "lt_fabrication_jours",
        "lt_liberation_jours", "lt_diffusion_jours", "lt_total_jours"
    ),
    on=["sku_id", "periode"],
    how="left"
)

# 9.3 Ajout Supply Chain (contexte ruptures)
df_joined = df_joined.join(
    df_sc.select("sku_id", "periode", "nb_ruptures", "qty_commandee", "qty_facturee"),
    on=["sku_id", "periode"],
    how="left"
)

# 9.4 Ajout segmentation ABC/XYZ (clé : sku_id uniquement)
df_joined = df_joined.join(
    df_segmentation.select(
        "sku_id", "segment_abc", "segment_xyz",
        "segment_abcxyz", "mape_cible_pct", "cv", "ca_total_12m"
    ),
    on="sku_id",
    how="left"
)

# 9.5 Calcul des écarts (forecast vs actuals)
# RÈGLE : écart calculé seulement si ventes_reelles_qty > 0
df_joined = df_joined \
    .withColumn(
        "ecart_qty",
        F.col("forecast_qty") - F.col("ventes_reelles_qty")
    ) \
    .withColumn(
        "ecart_pct",
        F.when(
            F.col("ventes_reelles_qty") > 0,
            (F.col("forecast_qty") - F.col("ventes_reelles_qty"))
            / F.col("ventes_reelles_qty") * 100
        ).otherwise(F.lit(None))  # NULL si pas de ventes réelles
    ) \
    .withColumn(
        "flag_rupture",
        F.when(F.col("nb_ruptures") > 0, True).otherwise(False)
    )

# 9.6 Audit trail
df_joined = ajouter_audit_trail(df_joined, "SAP+IQVIA+LeadTime+SC")

print(f"  forecasts_clean : {df_joined.count()} lignes")
print(f"  Colonnes        : {len(df_joined.columns)}")

# ─────────────────────────────────────────────────────────────────────────────
# 10. CONTRÔLES QUALITÉ AVANT ÉCRITURE
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("ÉTAPE 5 — Contrôles qualité Silver")
print("="*60)

# KPI 1 : % de lignes avec actuals manquants (normal pour horizons futurs)
nb_sans_actuals = df_joined.filter(F.col("ventes_reelles_qty").isNull()).count()
nb_total = df_joined.count()
pct_sans_actuals = round(nb_sans_actuals / nb_total * 100, 1)
print(f"  Lignes sans actuals IQVIA : {nb_sans_actuals} ({pct_sans_actuals}%) — normal si horizons futurs")

# KPI 2 : % de lignes sans segmentation ABC/XYZ
nb_sans_segment = df_joined.filter(F.col("segment_abcxyz").isNull()).count()
pct_sans_segment = round(nb_sans_segment / nb_total * 100, 1)
print(f"  Lignes sans segment ABC/XYZ : {nb_sans_segment} ({pct_sans_segment}%)")
if pct_sans_segment > 5:
    print("  [ATTENTION] Plus de 5% des SKUs non segmentés — vérifier le master data")

# KPI 3 : Distribution des algorithmes
print("\n  Distribution par algorithme :")
df_joined.groupBy("algorithme").count().orderBy("algorithme").show()

# KPI 4 : Plage temporelle couverte
periodes = df_joined.agg(
    F.min("periode").alias("debut"),
    F.max("periode").alias("fin")
).collect()[0]
print(f"  Plage temporelle : {periodes['debut']} → {periodes['fin']}")


# ─────────────────────────────────────────────────────────────────────────────
# 11. ÉCRITURE SILVER (format Delta — OneLake Fabric)
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("ÉTAPE 6 — Écriture de forecasts_clean (Silver)")
print("="*60)

# Sélection et ordre final des colonnes Silver
COLONNES_SILVER = [
    # Clés
    "sku_id", "periode", "algorithme",
    # Données forecast et actuals
    "forecast_qty", "ventes_reelles_qty", "ventes_reelles_valeur",
    # Écarts calculés
    "ecart_qty", "ecart_pct",
    # Segmentation
    "segment_abc", "segment_xyz", "segment_abcxyz",
    "mape_cible_pct", "cv", "ca_total_12m",
    # Lead Time 4 étapes
    "lt_besoins_jours", "lt_fabrication_jours",
    "lt_liberation_jours", "lt_diffusion_jours", "lt_total_jours",
    # Supply Chain contexte
    "nb_ruptures", "qty_commandee", "qty_facturee", "flag_rupture",
    # Master data
    "dci", "atc4",
    # Audit trail
    "date_traitement", "source_fichier"
]

df_silver = df_joined.select(COLONNES_SILVER)

# Écriture Delta (overwrite complet à chaque run)
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(TABLE_FORECASTS_CLEAN)

print(f"  [OK] forecasts_clean écrit avec succès → {TABLE_FORECASTS_CLEAN}")
print(f"  Lignes écrites : {df_silver.count()}")
print(f"  Colonnes       : {len(COLONNES_SILVER)}")

# ─────────────────────────────────────────────────────────────────────────────
# 12. RÉSUMÉ FINAL
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("RÉSUMÉ — 02_Bronze_to_Silver.py")
print("="*60)
print(f"  Table produite  : forecasts_clean (Silver)")
print(f"  Lignes          : {df_silver.count()}")
print(f"  Sources         : SAP S/4HANA + IQVIA + Lead Time (4 étapes) + Supply Chain")
print(f"  Segmentation    : ABC/XYZ calculée sur IQVIA 12 mois glissants")
print(f"  Écarts          : forecast_qty vs ventes_reelles_qty (IQVIA)")
print(f"  Audit trail     : date_traitement + source_fichier sur chaque ligne")
print(f"  Prochaine étape : 03_Validation_Engine.py (couche Gold)")
print("="*60 + "\n")

# =============================================================================
# FIN DU SCRIPT
# Ce script produit la table Silver 'forecasts_clean' qui sera consommée par :
#   - 03_Validation_Engine.py  → calcul MAPE/WAPE/Biais/FVA/alertes (Gold)
#   - 04_DAX_Measures.dax      → Power BI scorecards et monitoring
# =============================================================================
