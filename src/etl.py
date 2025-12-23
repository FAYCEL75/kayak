from __future__ import annotations
from pathlib import Path
import time
import pandas as pd

from scrapers.booking_scraper import scrape_booking
from utils import (
    ROOT, RAW, PROC, FIG,
    HardFailure, geocode_cities, fetch_weather,
    compute_destination_score, make_maps,
    upload_file_to_s3, load_to_rds
)

# -------------------------------------------------------------
# LISTE DES VILLES
# -------------------------------------------------------------
CITIES = [
    "Mont Saint Michel","St Malo","Bayeux","Le Havre","Rouen","Paris","Amiens",
    "Lille","Strasbourg","Chateau du Haut Koenigsbourg","Colmar","Eguisheim",
    "Besancon","Dijon","Annecy","Grenoble","Lyon","Gorges du Verdon",
    "Bormes les Mimosas","Cassis","Marseille","Aix en Provence","Avignon",
    "Uzes","Nimes","Aigues Mortes","Saintes Maries de la mer","Collioure",
    "Carcassonne","Ariege","Toulouse","Montauban","Biarritz","Bayonne","La Rochelle"
]

MAX_HOTELS_PER_CITY = 20
WEATHER_DAYS = 7


# ============================================================
# 1) GÉOCODAGE
# ============================================================
def step_geocoding() -> pd.DataFrame:
    print("🌍 Géocodage...")
    geo_path = RAW / "geocoding.csv"

    if geo_path.exists():
        df_geo = pd.read_csv(geo_path)
        print(f"✅ {len(df_geo)} villes géocodées (cache)")
        return df_geo

    df_geo = geocode_cities(CITIES)
    df_geo.to_csv(geo_path, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df_geo)} villes géocodées (Nominatim)")
    return df_geo


# ============================================================
# 2) MÉTÉO
# ============================================================
def step_weather(df_geo: pd.DataFrame) -> pd.DataFrame:
    print("⛅ Météo...")
    weather_path = RAW / "weather_raw.csv"

    if weather_path.exists():
        df_weather = pd.read_csv(weather_path)
        print(f"✅ {len(df_weather)} lignes météo (cache)")
        return df_weather

    df_weather = fetch_weather(df_geo, WEATHER_DAYS)
    df_weather.to_csv(weather_path, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df_weather)} lignes météo (API)")
    return df_weather


# ============================================================
# 3) SCRAPING BOOKING
# ============================================================
def step_scraping() -> pd.DataFrame:
    print("🏨 Scraping Booking LIVE...")

    hotels_all = []
    t0 = time.time()

    for city in CITIES:
        rows = scrape_booking(city, max_hotels=MAX_HOTELS_PER_CITY, retries=3)
        if rows:
            hotels_all.extend(rows)

    df_hotels = pd.DataFrame(hotels_all)

    # Sécurisation colonnes
    expected_cols = ["city", "hotelName", "score", "price_eur", "url"]
    for c in expected_cols:
        if c not in df_hotels.columns:
            df_hotels[c] = None

    # Normalisation score en float
    df_hotels["score_num"] = (
        df_hotels["score"].astype(str).str.replace(",", ".", regex=False)
    )
    df_hotels["score_num"] = pd.to_numeric(df_hotels["score_num"], errors="coerce")

    hotels_raw = RAW / "hotels_raw.csv"
    df_hotels.to_csv(hotels_raw, index=False, encoding="utf-8-sig")

    dt = time.time() - t0
    print(f"✅ {len(df_hotels)} hôtels scrapés en {dt/60:.1f} minutes")

    return df_hotels


# ============================================================
# 4) AGGREGATION DESTINATIONS
# ============================================================
def step_aggregation(df_geo: pd.DataFrame, df_weather: pd.DataFrame, df_hotels: pd.DataFrame):
    print("📈 Calcul des scores...")

    # météo agrégée
    df_w = df_weather.groupby("city", as_index=False).agg(
        temp_mean=("temp_day", "mean"),
        rain_sum=("rain", "sum"),
    )

    # hôtels agrégés
    df_h = df_hotels.groupby("city", as_index=False).agg(
        price_mean=("price_eur", "mean"),
        score_mean=("score_num", "mean"),
    )

    # merge météo + hotels + géo
    df_dest = df_w.merge(df_h, on="city", how="left").merge(
        df_geo[["city", "lat", "lon"]], on="city", how="left"
    )

    # Score final sécurisé
    def safe_score(row):
        try:
            return compute_destination_score(
                temp_mean=row["temp_mean"],
                rain_sum=row["rain_sum"],
                price_mean=row["price_mean"],
            )
        except Exception:
            return 0.0

    df_dest["destination_score"] = df_dest.apply(safe_score, axis=1)
    df_dest = df_dest.sort_values("destination_score", ascending=False).reset_index(drop=True)
    df_dest.insert(0, "rank", df_dest.index + 1)

    # Sauvegarde processed
    dest_path = PROC / "destinations_score.csv"
    hotels_clean = PROC / "hotels_clean.csv"

    df_dest.to_csv(dest_path, index=False, encoding="utf-8-sig")
    df_hotels.to_csv(hotels_clean, index=False, encoding="utf-8-sig")

    print("✅ Scores générés")

    return df_dest, dest_path, hotels_clean


# ============================================================
# 5) CARTES
# ============================================================
def step_maps(df_geo: pd.DataFrame, df_dest: pd.DataFrame, df_hotels: pd.DataFrame):
    print("🗺️ Génération cartes...")
    make_maps(df_geo=df_geo, df_dest=df_dest, df_hotels=df_hotels)
    print("✅ Cartes générées")


# ============================================================
# 6) S3
# ============================================================
def step_s3(df_paths: dict):
    print("☁️ Upload S3...")
    try:
        for local_path, key in df_paths.items():
            upload_file_to_s3(local_path, key)
        print("☁️ Upload OK")
    except Exception as e:
        print(f"[ERR] S3: {e}")


# ============================================================
# 7) RDS
# ============================================================
def step_rds(df_dest: pd.DataFrame, df_hotels: pd.DataFrame):
    print("🗄️ RDS...")
    try:
        load_to_rds(df_dest=df_dest, df_hotels=df_hotels)
    except Exception as e:
        # Ici on ne remet PAS en cause ton URI, on log juste l’erreur runtime
        print(f"[ERR] RDS (to_sql / connexion runtime): {e}")


# ============================================================
# PIPELINE COMPLET
# ============================================================
def main():
    print("🚀 Pipeline Kayak complet")

    # 1) GÉOCODAGE
    df_geo = step_geocoding()

    # 2) MÉTÉO
    df_weather = step_weather(df_geo)

    # 3) SCRAPING BOOKING
    df_hotels = step_scraping()

    # 4) AGGREGATION
    df_dest, dest_path, hotels_clean = step_aggregation(df_geo, df_weather, df_hotels)

    # 5) CARTES
    step_maps(df_geo, df_dest, df_hotels)

    # 6) S3 (optionnel selon tes ENV)
    geo_path = RAW / "geocoding.csv"
    weather_path = RAW / "weather_raw.csv"
    hotels_raw = RAW / "hotels_raw.csv"

    s3_files = {
        geo_path: "bloc1_kayak/geocoding.csv",
        weather_path: "bloc1_kayak/weather_raw.csv",
        hotels_raw: "bloc1_kayak/hotels_raw.csv",
        dest_path: "bloc1_kayak/destinations_score.csv",
        hotels_clean: "bloc1_kayak/hotels_clean.csv",
    }
    step_s3(s3_files)

    # 7) RDS
    step_rds(df_dest, df_hotels)

    print("🎉 Pipeline terminé sans erreur (logique) !")


if __name__ == "__main__":
    main()
from scrapers.booking_scraper import GLOBAL_DRIVER
if GLOBAL_DRIVER:
    GLOBAL_DRIVER.quit()