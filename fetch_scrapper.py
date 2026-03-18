import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import time
import string
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def clean_date(date_text):
    """Transforme 'Sep. 23, 2006' en '2006-09-23'."""
    try:
        # On enlève les points après les mois (ex: Sep. -> Sep)
        clean_text = date_text.replace('.', '').strip()
        # Conversion en objet date puis en texte ISO
        date_obj = datetime.strptime(clean_text, "%b %d, %Y")
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        return None # Si la date est illisible, on laisse vide

def clean_float(text):
    if not text or "--" in text: return 0.0
    clean_text = text.replace('"', '').replace('%', '').strip()
    try: return float(clean_text)
    except ValueError: return 0.0

def scrape_fights(fighter_url, fighter_id, supabase):
    response = requests.get(fighter_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    fight_rows = soup.find_all('tr', class_='b-fight-details__table-row')[1:]
    
    for row in fight_rows:
        cols = row.find_all('td')
        if len(cols) >= 7:
            try:
                # 1. NETTOYAGE DU RÉSULTAT
                # On prend le texte, on enlève les espaces et on met en minuscules
                raw_res = cols[0].text.strip().lower()
                result = 'win' if 'win' in raw_res else 'loss' if 'loss' in raw_res else 'draw'

                # 2. NETTOYAGE DE L'ADVERSAIRE (Le point bloquant !)
                # L'adversaire est dans le 2ème <td>, généralement dans le 2ème paragraphe
                opponent_p = cols[1].find_all('p')
                # Si on trouve plusieurs <p>, le deuxième est souvent l'adversaire
                opponent_name = opponent_p[1].text.strip() if len(opponent_p) > 1 else cols[1].text.strip()
                
                # 3. DATE ET ROUND
                raw_date = cols[6].find_all('p')[1].text.strip()
                formatted_date = clean_date(raw_date)
                
                round_val = cols[4].text.strip()
                round_int = int(round_val) if round_val.isdigit() else 0

# Nettoyage des champs techniques
                method = cols[3].find_all('p')[0].text.strip()
                if method == "--" or not method:
                    method = "N/A"
                
                event = cols[6].find_all('p')[0].text.strip()
                if not event or event == "--":
                    event = "UFC Event"

                time_val = cols[5].text.strip()
                if time_val == "--":
                    time_val = "0:00"

                fight_data = {
                    "fighter_id": fighter_id,
                    "result": result, # 'win', 'loss', 'draw'
                    "opponent_name": opponent_name[:255],
                    "method": method, # On envoie "N/A" au lieu de "--"
                    "round": round_int,
                    "time": time_val,
                    "event_name": event,
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
