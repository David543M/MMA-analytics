import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def clean_float(text):
    """Nettoie le texte pour en faire un nombre propre."""
    if not text or "--" in text:
        return 0.0
    # Enlève les guillemets, les % et les espaces
    clean_text = text.replace('"', '').replace('%', '').strip()
    try:
        return float(clean_text)
    except ValueError:
        return 0.0

def scrape_ufc_fighters():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erreur : Clés Supabase manquantes.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Démarrage du scraping...")
    
    # On boucle sur quelques lettres pour tester (A, B, C)
    for char in ['a', 'b', 'c']:
        print(f"Extraction de la lettre : {char.upper()}")
        url = f"http://ufcstats.com/statistics/fighters?char={char}&page=all"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.find_all('tr', class_='b-statistics__table-row')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 10:
                try:
                    fighter_data = {
                        "name": f"{cols[0].text.strip()} {cols[1].text.strip()}",
                        "nickname": cols[2].text.strip(),
                        "slpm": clean_float(cols[5].text),
                        "str_acc": clean_float(cols[6].text),
                        "sapm": clean_float(cols[7].text),
                        "str_def": clean_float(cols[8].text),
                        "td_avg": clean_float(cols[9].text),
                        "violence_score": 50
                    }
                    supabase.table("fighters").upsert(fighter_data).execute()
                    print(f"Succès : {fighter_data['name']}")
                except Exception as e:
                    print(f"Erreur sur {cols[0].text.strip()} : {e}")

if __name__ == "__main__":
    scrape_ufc_fighters()
