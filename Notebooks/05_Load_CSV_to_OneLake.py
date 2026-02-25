# =============================================================================
# FAAS4U — Laprophan | Script PySpark pour OneLake (Microsoft Fabric)
# Fichier  : 05_Load_CSV_to_OneLake.py
# Auteure  : Salma | PFE Mundiapolis 2026
# Rôle     : Charger manuellement les fichiers de test .csv (Couche Gold)
#            dans les bases de données Delta Lake de OneLake.
#            Permet la connexion DirectLake dans Power BI!
# =============================================================================
# PRÉ-REQUIS FABRIC :
# 1. Allez dans votre Workspace Fabric Laprophan.
# 2. Ouvrez le Lakehouse cible de votre projet FAAS4U (ex: 'Lakehouse_FAAS4U').
# 3. Cliquez sur "Files" (Fichiers) -> "Upload" (Charger) 
#    -> Sélectionnez vos 4 fichiers CSV présents dans Data_Test/.
# 4. Ouvrez un nouveau Notebook PySpark et copiez ce code.
# =============================================================================

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import * # type: ignore
import pyspark.sql.functions as F # type: ignore

# 1. Initialiser (Fabric gère cette session par défaut, code de pré-caution)
try:
    spark = SparkSession.builder.getOrCreate()
except:
    pass

# =============================================================================
# VARIABLES : Chemins des fichiers uploaddés (dossier interne au Lakehouse)
# =============================================================================
# Par défaut, Fabric monte les fichiers uploadés manuellement dans "Files/"
FILES_PATH = "Files/" 
DELTA_GOLD_PATH = "Tables/gold/"
DELTA_SILVER_PATH = "Tables/silver/"

FILE_FORECASTS = FILES_PATH + "forecasts_clean.csv"
FILE_PERFORMANCE = FILES_PATH + "performance_metrics.csv"
FILE_FVA = FILES_PATH + "fva_results.csv"
FILE_ALERTES = FILES_PATH + "alertes_derive.csv"


# =============================================================================
# FONCTION DE CHARGEMENT CSV -> DELTA
# =============================================================================
def load_csv_to_delta(csv_path: str, delta_table_name: str, mode: str = "overwrite"):
    """
    Charge un CSV depuis le dossier Files/ et l'écrit au format Delta 
    dans la racine Tables/ du Lakehouse FAAS4U.
    """
    print(f"\n[INFO] Traitement du fichier : {csv_path}")
    
    try:
        # Lire le fichier CSV (inferSchema est lourd mais utile pour les tests)
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(csv_path)
            
        lignes = df.count()
        print(f"  -> Lignes lues : {lignes}")
        
        # Sauvegarde en format Delta
        df.write.format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .save(delta_table_name)
            
        print(f"  ✅ Succès : Table Delta créée/écrasée : {delta_table_name}")
        
    except Exception as e:
        print(f"  ❌ Erreur lors du chargement de {csv_path} : {str(e)}")


# =============================================================================
# EXÉCUTION DU PIPELINE DE CHARGEMENT
# =============================================================================
print("===================================================================")
print(" DÉBUT DU CHARGEMENT DES DONNÉES DE TEST FAAS4U VERS ONELAKE")
print("===================================================================")

# 1. Charger forecasts_clean en Couche Silver
load_csv_to_delta(
    csv_path=FILE_FORECASTS,
    delta_table_name=f"{DELTA_SILVER_PATH}forecasts_clean"
)

# 2. Charger les KPIs en Couche Gold
load_csv_to_delta(
    csv_path=FILE_PERFORMANCE,
    delta_table_name=f"{DELTA_GOLD_PATH}performance_metrics"
)

# 3. Charger les résultats FVA en Couche Gold
load_csv_to_delta(
    csv_path=FILE_FVA,
    delta_table_name=f"{DELTA_GOLD_PATH}fva_results"
)

# 4. Charger les alertes en Couche Gold
load_csv_to_delta(
    csv_path=FILE_ALERTES,
    delta_table_name=f"{DELTA_GOLD_PATH}alertes_derive"
)

print("\n===================================================================")
print(" TERMINE. VOS TABLES GOLD SONT MAINTENANT DISPONIBLES DANS ONELAKE.")
print(" Vous pouvez désormais brancher Power BI en DirectLake !")
print("===================================================================")

# =============================================================================
# REQUÊTE SQL (OPT) : Valider les données directement dans le Notebook
# =============================================================================
# Vous pouvez faire tourner ces cellules à la volée une fois le delta créé.
"""
%%sql
SELECT * FROM gold.performance_metrics ORDER BY mape ASC LIMIT 5
"""
