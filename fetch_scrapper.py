import os
import string
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from supabase import create_client


SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def clean_date(date_text):
    """Transforme 'Sep. 23, 2006' en '2006-09-23'."""
    try:
        clean_text = date_text.replace(".", "").strip()
        date_obj = datetime.strptime(clean_text, "%b %d, %Y")
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        return None


def clean_float(text):
    """Nettoie les pourcentages et les tirets pour les statistiques."""
    if not text or "--" in text:
        return 0.0
    clean_text = text.replace('"', "").replace("%", "").strip()
    try:
        return float(clean_text)
    except ValueError:
        return 0.0


def clean_int(text):
    """Nettoie une valeur entière."""
    if not text or "--" in text:
        return 0

    clean_text = " ".join(text.split()).strip()

    try:
        return int(clean_text)
    except ValueError:
        digits = "".join(ch for ch in clean_text if ch.isdigit())
        return int(digits) if digits else 0


def scrape_fights(fighter_url, fighter_id, supabase):
    """Récupère les vraies stats techniques et l'historique des combats d'un profil UFC."""
    response = requests.get(fighter_url, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")

    tech_stats = {
        "slpm": 0.0,
        "str_acc": 0.0,
        "sapm": 0.0,
        "str_def": 0.0,
        "td_avg": 0.0,
        "td_acc": 0.0,
        "td_def": 0.0,
        "sub_avg": 0.0,
    }

    stat_map = {
        "SLpM:": "slpm",
        "Str. Acc.:": "str_acc",
        "SApM:": "sapm",
        "Str. Def:": "str_def",
        "TD Avg.:": "td_avg",
        "TD Acc.:": "td_acc",
        "TD Def.:": "td_def",
        "Sub. Avg.:": "sub_avg",
    }

    for item in soup.find_all("li", class_="b-list__box-list-item"):
        text = " ".join(item.get_text(" ", strip=True).split())
        for label, field in stat_map.items():
            if text.startswith(label):
                value = text.split(label, 1)[1].strip()
                tech_stats[field] = clean_float(value)
                break

    supabase.table("fighters").update(tech_stats).eq("id", fighter_id).execute()

    fight_rows = soup.find_all("tr", class_="b-fight-details__table-row")[1:]

    for row in fight_rows:
        cols = row.find_all("td")
        if len(cols) >= 10:
            try:
                raw_res = cols[0].text.strip().lower()
                if "win" in raw_res:
                    result = "win"
                elif "loss" in raw_res:
                    result = "loss"
                elif "draw" in raw_res:
                    result = "draw"
                elif "nc" in raw_res:
                    result = "nc"
                else:
                    result = "loss"

                opponent_p = cols[1].find_all("p")
                raw_opp = opponent_p[1].text if len(opponent_p) > 1 else cols[1].text
                opponent_name = " ".join(raw_opp.split())[:255]

                # Colonne STR = strikes fighter / strikes opponent
                strikes_p = cols[3].find_all("p")
                strikes_landed = clean_int(strikes_p[0].text) if len(strikes_p) > 0 else 0
                strikes_absorbed = clean_int(strikes_p[1].text) if len(strikes_p) > 1 else 0

                event_p = cols[6].find_all("p")
                raw_event = event_p[0].text if event_p else "UFC Event"
                event_name = " ".join(raw_event.split())
                if not event_name or event_name == "--":
                    event_name = "UFC Event"

                raw_date = event_p[1].text if len(event_p) > 1 else ""
                formatted_date = clean_date(raw_date)

                raw_method = cols[7].find_all("p")[0].text if cols[7].find_all("p") else cols[7].text
                method = " ".join(raw_method.split())
                if not method or method == "--":
                    method = "N/A"

                raw_round = cols[8].text.strip()
                round_int = int(raw_round) if raw_round.isdigit() else 1

                time_val = cols[9].text.strip()
                if not time_val or time_val == "--":
                    time_val = "0:00"

                fight_data = {
                    "fighter_id": fighter_id,
                    "result": result,
                    "opponent_name": opponent_name,
                    "method": method,
                    "round": round_int,
                    "time": time_val,
                    "event_name": event_name,
                    "date": formatted_date,
                    "strikes_landed": strikes_landed,
                    "strikes_absorbed": strikes_absorbed,
                }

                if formatted_date:
                    existing = (
                        supabase.table("fights")
                        .select("id")
                        .eq("fighter_id", fighter_id)
                        .eq("date", formatted_date)
                        .eq("opponent_name", opponent_name)
                        .execute()
                    )

                    if existing.data:
                        fight_row_id = existing.data[0]["id"]
                        supabase.table("fights").update(fight_data).eq("id", fight_row_id).execute()
                    else:
                        supabase.table("fights").insert(fight_data).execute()

            except Exception as e:
                print(f"      Erreur technique combat : {e}")


def scrape_ufc_fighters():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Erreur : Clés Supabase manquantes.")
        return

    print("Demarrage du scraping global...")

    for char in string.ascii_lowercase:
        print(f"--- Lettre : {char.upper()} ---")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        url = f"http://ufcstats.com/statistics/fighters?char={char}&page=all"
        response = requests.get(url, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr", class_="b-statistics__table-row")[1:]

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 10:
                full_name = "Inconnu"
                try:
                    fighter_link = cols[0].find("a")["href"]
                    full_name = f"{' '.join(cols[0].text.split())} {' '.join(cols[1].text.split())}"

                    fighter_info = {
                        "name": full_name,
                        "nickname": " ".join(cols[2].text.split()),
                        "division": "UFC",
                        "violence_score": 50,
                    }

                    existing = supabase.table("fighters").select("id").eq("name", full_name).execute()

                    if existing.data:
                        f_id = existing.data[0]["id"]
                        print(f"Deja en base : {full_name} (ID: {f_id})")
                    else:
                        res = supabase.table("fighters").insert(fighter_info).execute()
                        if res.data:
                            f_id = res.data[0]["id"]
                            print(f"Ajout : {full_name} (ID: {f_id})")
                        else:
                            continue

                    scrape_fights(fighter_link, f_id, supabase)
                    time.sleep(0.1)

                except Exception as e:
                    print(f"Erreur sur {full_name if 'full_name' in locals() else 'Ligne'} : {e}")


if __name__ == "__main__":
    scrape_ufc_fighters()
