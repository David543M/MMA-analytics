import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import time
import string
from datetime import datetime

# Configuration Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def clean_date(date_text):
    """Transforme 'Sep. 23, 2006' en '2006-09-23'."""
    try:
        clean_text = date_text.replace('.', '').strip()
        date_obj = datetime.strptime(clean_text, "%b %d, %Y")
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        return None

def clean_float(text):
    if not text or "--" in text: return 0.0
    clean_text = text.replace('"', '').replace('%', '').strip()
    try: return float(clean_text)
    except ValueError: return 0.0

def scrape_fights(fighter_url, fighter_id, supabase):
    """Récupère et nettoie les combats pour un combattant donné."""
    response = requests.get(fighter_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    fight_rows = soup.find_all('tr', class_='b-fight-details__table-row')[1:]
    
    for row in fight_rows:
        cols = row.find_all('td')
        if len(cols) >= 7:
            try:
                # 1. NETTOYAGE DU RÉSULTAT (Format 'Win', 'Loss', 'Draw' pour la contrainte)
                raw_res = cols[0].text.strip().lower()
                if 'win' in raw_res:
                    result = 'Win'
                elif 'loss' in raw_res:
                    result = 'Loss'
                elif 'draw' in raw_res:
                    result = 'Draw'
                else:
                    result = 'Loss'

                # 2. EXTRACTION PROPRE DE L'ADVERSAIRE
                opponent_p = cols[1].find_all('p')
                opponent_name = opponent_p[1].text.strip() if len(opponent_p) > 1 else cols[1].text.strip()

                # 3. NETTOYAGE DE L'ÉVÉNEMENT ET DE LA DATE
                event_p = cols[6].find_all('p')
                event_name = event_p[0].text.strip() if event_p else "UFC Event"
                if event_name == "--" or not event_name:
                    event_name = "UFC Event"
                
                raw_date = event_p[1].text.strip() if len(event_p) > 1 else ""
                formatted_date = clean_date(raw_date)

                # 4. NETTOYAGE DE LA MÉTHODE, DU ROUND ET DU TEMPS
                method_raw = cols[3].find_all('p')[0].text.strip()
                method = method_raw if method_raw != "--" else "N/A"
                
                round_val = cols[4].text.strip()
                round_int = int(round_val) if round_val.isdigit() else 0
                
                time_val = cols[5].text.strip()
                if time_val == "--": time_val = "0:00"

                # 5. PRÉPARATION DES DONNÉES
                fight_data = {
                    "fighter_id": fighter_id,
                    "result": result,
                    "opponent_name": opponent_name[:255].strip(),
                    "method": method,
                    "round": round_int,
                    "time": time_val,
                    "event_name": event_name,
                    "date": formatted_date
                }
                
                if formatted_date:
                    supabase.table("fights").upsert(fight_data).execute()
                    
            except Exception as e:
                print(f"      Erreur technique combat : {e}")

def scrape_ufc_fighters():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erreur : Clés Supabase manquantes.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("🚀 Démarrage du scraping global...")
    
    for char in string.ascii_lowercase:
        print(f"--- Lettre : {char.upper()} ---")
        url = f"http://ufcstats.com/statistics/fighters?char={char}&page=all"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr', class_='b-statistics__table-row')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 10:
                full_name = "Inconnu"
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
                    
                    # Upsert du fighter et récupération de l'UUID
                    res = supabase.table("fighters").upsert(fighter_info).execute()
                    if res.data:
                        f_id = res.data[0]['id']
                        print(f"✅ Fighter : {full_name} (ID: {f_id})")
                        scrape_fights(fighter_link, f_id, supabase)
                    
                    time.sleep(0.1) # Respect du serveur
                    
                except Exception as e:
                    print(f"❌ Erreur sur {full_name} : {e}")

if __name__ == "__main__":
    scrape_ufc_fighters()
