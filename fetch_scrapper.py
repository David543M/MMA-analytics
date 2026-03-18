import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import time
import string

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def clean_float(text):
    if not text or "--" in text: return 0.0
    clean_text = text.replace('"', '').replace('%', '').strip()
    try: return float(clean_text)
    except ValueError: return 0.0

def scrape_fights(fighter_url, fighter_id, supabase):
    """Récupère les combats et les lie via l'ID unique de Supabase."""
    response = requests.get(fighter_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    fight_rows = soup.find_all('tr', class_='b-fight-details__table-row')[1:]
    
    for row in fight_rows:
        cols = row.find_all('td')
        if len(cols) >= 7:
            try:
                # Nettoyage du round pour correspondre au type int4 de ta table
                round_val = cols[4].text.strip()
                round_int = int(round_val) if round_val.isdigit() else 0

                fight_data = {
                    "fighter_id": fighter_id, # L'ID UUID récupéré
                    "result": cols[0].text.strip(),
                    "opponent_name": cols[1].text.strip(),
                    "method": cols[3].find_all('p')[0].text.strip(),
                    "round": round_int,
                    "time": cols[5].text.strip(),
                    "event_name": cols[6].text.strip(),
                    "date": cols[6].find_all('p')[1].text.strip()
                }
                supabase.table("fights").upsert(fight_data).execute()
            except Exception as e:
                print(f"      Erreur combat : {e}")

def scrape_ufc_fighters():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erreur : Clés Supabase manquantes.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("🚀 Démarrage du scraping global...")
    
    # On boucle sur tout l'alphabet
    for char in string.ascii_lowercase:
        print(f"--- Lettre : {char.upper()} ---")
        url = f"http://ufcstats.com/statistics/fighters?char={char}&page=all"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr', class_='b-statistics__table-row')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 10:
                try:
                    fighter_link = cols[0].find('a')['href']
                    full_name = f"{cols[0].text.strip()} {cols[1].text.strip()}"
                    
                    fighter_info = {
                        "name": full_name,
                        "nickname": cols[2].text.strip(),
                        "division": "UFC",
                        "slpm": clean_float(cols[5].text),
                        "str_acc": clean_float(cols[6].text),
                        "sapm": clean_float(cols[7].text),
                        "str_def": clean_float(cols[8].text),
                        "td_avg": clean_float(cols[9].text),
                        "violence_score": 50
                    }
                    
                    # 1. On enregistre le fighter et on RÉCUPÈRE son ID (UUID)
                    res = supabase.table("fighters").upsert(fighter_info).execute()
                    f_id = res.data[0]['id'] # C'est cet ID qu'on utilise pour la table fights
                    
                    print(f"✅ Fighter : {full_name} (ID: {f_id})")
                    
                    # 2. On scrape ses combats en lui donnant cet ID
                    scrape_fights(fighter_link, f_id, supabase)
                    
                    time.sleep(0.1) # Sécurité anti-ban
                    
                except Exception as e:
                    print(f"❌ Erreur sur {full_name if 'full_name' in locals() else 'Ligne'} : {e}")

if __name__ == "__main__":
    scrape_ufc_fighters()
