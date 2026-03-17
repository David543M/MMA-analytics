import pandas as pd
from supabase import create_client

# Tes accès Supabase (trouvés dans Settings > API)
URL = "TA_SUPABASE_URL"
KEY = "TA_SUPABASE_SERVICE_ROLE_KEY"
supabase = create_client(URL, KEY)

def upload_fighters():
    # Ici, on simule la donnée récupérée (on peut utiliser une lib de scraping)
    # Pour le test, on crée un dictionnaire
    data = [
        {"name": "Jon Jones", "division": "Heavyweight", "violence_score": 85},
        # ... le script bouclera sur les pages de UFCStats ici
    ]
    
    # Envoi vers Supabase
    for fighter in data:
        supabase.table("fighters").upsert(fighter).execute()
        print(f"Updated: {fighter['name']}")

if __name__ == "__main__":
    upload_fighters()