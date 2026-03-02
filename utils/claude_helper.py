# =============================================================================
# FAAS4U — Laprophan | Script Helper API Claude
# Fichier  : utils/claude_helper.py
# Auteure  : Salma | PFE Mundiapolis 2026
# Rôle     : Fournit une abstraction sécurisée pour appeler l'API Claude,
#            compatible avec un .env (en local) ou Fabric Key Vault (en cloud).
# =============================================================================

import os
import json
import requests
import time

# Options pour le cloud Fabric (si exécuté sur Notebook Fabric)
try:
    from notebookutils import mssparkutils # type: ignore
    is_fabric = True
except ImportError:
    # Si on est en local (Antigravity ou VSCode), mssparkutils n'existe pas.
    is_fabric = False
    from dotenv import load_dotenv # type: ignore
    load_dotenv()  # Charge les variables depuis .env local

# Paramètres API
CLAUDE_LATEST_MODEL = "claude-3-7-sonnet-20250219"
CLAUDE_STD_MODEL = "claude-3-5-sonnet-20241022"
API_URL = "https://api.anthropic.com/v1/messages"

def get_anthropic_api_key() -> str:
    """
    Récupère la clé de manière sécurisée selon l'environnement d'exécution.
    """
    api_key = None
    
    if is_fabric:
        # Dans Microsoft Fabric, on récupère souvent la clé d'un Key Vault Azure
        # ou via la librairie mssparkutils (Vault_FAAS4U est un exemple de nom).
        try:
            api_key = mssparkutils.credentials.getSecret("Vault_FAAS4U", "ANTHROPIC-API-KEY")
        except Exception as e:
            print(f"[ERREUR FABRIC SECRETS] Impossible de récupérer la clé : {str(e)}")
            # Fallback en cas où elle a été passée via la configuration de la session
            api_key = os.environ.get("ANTHROPIC_API_KEY")
    else:
        # En local, on la lit depuis les variables d'environnement (.env)
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        raise ValueError("""
        [ERREUR CRITIQUE] La clé Anthropic (ANTHROPIC_API_KEY) est introuvable.
        - En local : Vérifiez la présence de votre fichier .env
        - Sur Fabric : Vérifiez l'accès de votre Workspace au Key Vault
        """)
        
    return api_key


def ask_claude(prompt: str, system_prompt: str = None, max_tokens: int = 1000, model: str = CLAUDE_STD_MODEL) -> str:
    """
    Appelle de manière sécurisée l'API Anthropic (Claude).
    Gère gracieusement le rate limit et les erreurs HTTP.
    
    Args:
        prompt (str): Instruction principale ou données à soumettre à l'IA.
        system_prompt (str): Contexte système à donner (Optionnel).
        max_tokens (int): Longueur maximale de la réponse attendue.
        model (str): Le modèle Claude à utiliser.
        
    Returns:
        str: La réponse textuelle générée par Claude.
    """
    try:
        api_key = get_anthropic_api_key()
    except ValueError as ve:
        return str(ve)
        
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}]
    }
    
    if system_prompt:
        payload["system"] = system_prompt

    max_retries = 3
    retry_delay = 5 # Secondes

    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=headers, json=payload, timeout=45)
            
            if response.status_code == 200:
                result = response.json()
                # On retourne le texte du premier bloc de la réponse (souvent le seul bloc)
                return result["content"][0]["text"].strip()
                
            elif response.status_code == 429: # Rate limit
                print(f"  [ATTENTION] Rate limit dépassé (Statut 429). Tentative {attempt+1}/{max_retries}...")
                time.sleep(retry_delay * (attempt + 1))
                
            elif response.status_code == 401: # Auth Error
                return "[ERREUR API] Statut 401 : Clé API Anthropic invalide ou expirée."
                
            else:
                return f"[ERREUR API] Statut {response.status_code} : {response.text}"
                
        except requests.exceptions.RequestException as req_e:
            print(f"  [ATTENTION] Erreur réseau lors de l'appel. Tentative {attempt+1}/{max_retries} : {str(req_e)}")
            time.sleep(retry_delay)
            
    return "[ÉCHEC API] Délai ou erreurs persistantes après plusieurs tentatives."

# -----------------------------------------------------------------------------
# Test local expéditif si exécuté explicitement
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("Test de la configuration Claude API...")
    try:
        key = get_anthropic_api_key()
        print("✅ Clé API trouvée (Longueur: {})".format(len(key)))
        
        reponse = ask_claude(
            prompt="Génère une mesure DAX pour calculer le MAPE pondéré.",
            system_prompt="Tu es un expert Business Intelligence Power BI.",
            max_tokens=300
        )
        print("\n--- RÉPONSE CLAUDE ---")
        print(reponse)
        
    except Exception as e:
        print(f"❌ Erreur lors du test : {str(e)}")
