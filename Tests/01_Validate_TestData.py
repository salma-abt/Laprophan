# =============================================================================
# FAAS4U — Laprophan | Script de Validation des Données de Test
# Fichier  : 01_Validate_TestData.py
# Rôle     : S'assurer que les fichiers CSV générés en local correspondent 
#            aux schémas attendus dans Microsoft Fabric (Annexe B).
# =============================================================================

import os
import pandas as pd # type: ignore

LOCAL_DATA_DIR = r"C:\Users\H P\Documents\Laprophan\Data_Test"

# Définition des colonnes minimales requises basées sur la spécification FAAS4U
EXPECTED_COLUMNS = {
    "forecasts_clean.csv": [
        "sku_id", "periode", "algorithme", "forecast_qty", "ventes_reelles_qty",
        "ecart_pct", "segment_abcxyz", "lt_total_jours", "flag_rupture"
    ],
    "performance_metrics.csv": [
        "algorithme", "segment_abcxyz", "horizon_mois", "mape", "wape",
        "rmse", "biais_pct", "theil_u", "tracking_signal", "flag_derive"
    ],
    "fva_results.csv": [
        "segment_abcxyz", "horizon_mois", "mape_naive", "meilleur_algo",
        "mape_best_algo", "mape_dp", "fva_algo_pts", "fva_humain_pts", "verdict_humain"
    ],
    "alertes_derive.csv": [
        "algorithme", "segment_abcxyz", "type_alerte", "description_alerte",
        "recommandation_claude", "criticite", "responsable"
    ]
}

def validate_test_data():
    print("="*70)
    print(" 🛠️  VALIDATION DES FICHIERS DE TEST FAAS4U (Annexe B)")
    print("="*70)

    if not os.path.exists(LOCAL_DATA_DIR):
        print(f"❌ [ERREUR CRITIQUE] Le dossier cible n'existe pas : {LOCAL_DATA_DIR}")
        return

    all_passed = True

    for filename, required_cols in EXPECTED_COLUMNS.items():
        filepath = os.path.join(LOCAL_DATA_DIR, filename)
        
        print(f"\n▶ Vérification de : {filename}")
        
        if not os.path.exists(filepath):
            print(f"   ❌ [FICHIER INTROUVABLE] Le fichier n'existe pas dans Data_Test/")
            all_passed = False
            continue
            
        try:
            # On charge juste l'entête et 10 lignes pour aller vite
            df = pd.read_csv(filepath, nrows=10)
            actual_cols = df.columns.tolist()
            
            missing_cols = [col for col in required_cols if col not in actual_cols]
            
            if missing_cols:
                print(f"   ❌ [ÉCHEC SCHÉMA] Colonnes attendues manquantes :")
                for col in missing_cols:
                    print(f"      - {col}")
                all_passed = False
            else:
                print(f"   ✅ [SCHÉMA OK] 100% des colonnes attendues sont bien présentes.")
                print(f"   ℹ️  (Le fichier contient au total {len(actual_cols)} colonnes et a bien été lu)")
                
        except Exception as e:
            print(f"   ❌ [ERREUR LECTURE] Impossible de parser le CSV : {str(e)}")
            all_passed = False

    print("\n" + "="*70)
    if all_passed:
        print(" 🎉 BILAN : SUCCÈS TOTAL. Vos données de test sont prêtes pour")
        print("    l'importation on-premise vers Microsoft Fabric ! ")
    else:
        print(" ⚠️ BILAN : ÉCHEC PARTIEL. Veuillez corriger les erreurs ci-dessus")
        print("    avant de charger les données dans OneLake.")
    print("="*70 + "\n")

if __name__ == "__main__":
    validate_test_data()
