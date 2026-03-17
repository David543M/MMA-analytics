import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client

# Lecture des secrets
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def scrape_ufc_fighters():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erreur : Clés Supabase manquantes.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Démarrage du scraping...")
    
    url = "http://ufcstats.com/statistics/fighters?char=a&page=all"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    rows = soup.find_all('tr', class_='b-statistics__table-row')[1:]
    
    for row in rows:
        cols = row.find_all('td')
        # On vérifie qu'on a bien au moins 2 colonnes pour éviter le crash
        if len(cols) >= 2:
            try:
                fighter_data = {
                    "name": f"{cols[0].text.strip()} {cols[1].text.strip()}",
                    "nickname": cols[2].text.strip() if len(cols) > 2 else "",
                    "slpm": float(cols[5].text.strip() or 0) if len(cols) > 5 else 0,
                    "violence_score": 50
                }
                # Envoi à Supabase
                supabase.table("fighters").upsert(fighter_data).execute()
                print(f"Importé : {fighter_data['name']}")
            except Exception as e:
                print(f"Erreur sur une ligne : {e}")

if __name__ == "__main__":
    scrape_ufc_fighters()
