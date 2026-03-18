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
    """Nettoie les pourcentages et les tirets pour les statistiques."""
    if not text or "--" in text: return 0.0
    clean_text = text.replace('"', '').replace('%', '').strip()
    try: return float(clean_text)
    except ValueError: return 0.0

def scrape_fights(fighter_url, fighter_id, supabase):
    """Récupère et nettoie l'historique des combats d'un profil UFC."""
    response = requests.get(fighter_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    fight_rows = soup.find_all('tr', class_='b-fight-details__table-row')[1:]
    
    for row in fight_rows:
        cols = row.find_all('td')
        # La table des combats a 10 colonnes sur la page de profil
        if len(cols) >= 10:
            try:
                # 1. RÉSULTAT (Colonne 0)
                raw_res = cols[0].text.strip().lower()
                if 'win' in raw_res: result = 'win'
                elif 'loss' in raw_res: result = 'loss'
                elif 'draw' in raw_res: result = 'draw'
                elif 'nc' in raw_res: result = 'nc'
                else: result = 'loss'

                # 2. ADVERSAIRE (Colonne 1)
                opponent_p = cols[1].find_all('p')
                raw_opp = opponent_p[1].text if len(opponent_p) > 1 else cols[1].text
                opponent_name = " ".join(raw_opp.split())[:255]

                # 3. ÉVÉNEMENT & DATE (Colonne 6)
                event_p = cols[6].find_all('p')
                raw_event = event_p[0].text if event_p else "UFC Event"
                event_name = " ".join(raw_event.split())
                if not event_name or event_name == "--":
                    event_name = "UFC Event"
                
                raw_date = event_p[1].text if len(event_p) > 1 else ""
                formatted_date = clean_date(raw_date)

                # 4. MÉTHODE (Colonne 7 - C'était l'erreur majeure !)
                raw_method = cols[7].find_all('p')[0].text if cols[7].find_all('p') else cols[7].text
                method = " ".join(raw_method.split())
                if not method or method == "--": method = "N/A"
                
                # 5. ROUND (Colonne 8)
                raw_round = cols[8].text.strip()
                round_int = int(raw_round) if raw_round.isdigit() else 1
                
                # 6. TEMPS (Colonne 9)
                time_val = cols[9].text.strip()
                if not time_val or time_val == "--": time_val = "0:00"

                # 7. PRÉPARATION DES DONNÉES
                fight_data = {
                    "fighter_id": fighter_id,
                    "result": result,
                    "opponent_name": opponent_name,
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
    
    # Boucle sur tout l'alphabet
    for char in string.ascii_lowercase:
        print(f"--- Lettre : {char.upper()} ---")
        url = f"http://ufcstats.com/statistics/fighters?char={char}&page=all"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr', class_='b-statistics__table-row')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            # La table liste des combattants a 11 colonnes
            if len(cols) >= 10:
                full_name = "Inconnu"
                try:
                    fighter_link = cols[0].find('a')['href']
                    # Correction des guillemets ici :
                    full_name = f"{' '.join(cols[0].text.split())} {' '.join(cols[1].text.split())}"
                    
                    fighter_info = {
                        "name": full_name,
                        # Et correction des guillemets ici aussi :
                        "nickname": ' '.join(cols[2].text.split()),
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
                        # Lancement du scraping de ses combats
                        scrape_fights(fighter_link, f_id, supabase)
                    
                    time.sleep(0.1) # Respect du serveur
                    
                except Exception as e:
                    print(f"❌ Erreur sur {full_name if 'full_name' in locals() else 'Ligne'} : {e}")

if __name__ == "__main__":
    scrape_ufc_fighters()
