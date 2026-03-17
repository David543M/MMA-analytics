import requests
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Erreur : Les clés Supabase ne sont pas configurées dans les variables d'environnement.")
else:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape_ufc_fighters():
    print("Démarrage du scraping...")
    # Exemple pour la lettre 'A' sur UFC Stats
    url = "http://ufcstats.com/statistics/fighters?char=a&page=all"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    rows = soup.find_all('tr', class_='b-statistics__table-row')[1:] # On saute l'entête
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) > 0:
            fighter_data = {
                "name": f"{cols[0].text.strip()} {cols[1].text.strip()}",
                "nickname": cols[2].text.strip(),
                "division": "TBD", # UFC Stats ne donne pas la division sur cette page
                "slpm": float(cols[5].text.strip() or 0),
                "str_acc": int(cols[6].text.strip().replace('%', '') or 0),
                "violence_score": 50 # Valeur par défaut à calculer plus tard
            }
            
            # 2. On envoie direct dans Supabase
            supabase.table("fighters").upsert(fighter_data).execute()
            print(f"Importé : {fighter_data['name']}")

if __name__ == "__main__":
    scrape_ufc_fighters()
