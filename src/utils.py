import os
import json
import boto3
import pandas as pd
from pathlib import Path
import plotly.express as px
from sqlalchemy import create_engine

from config import (
    RDS_URI,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_BUCKET
)


# =====================================================================
# PATHS
# =====================================================================
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "reports" / "raw"
PROC = ROOT / "reports" / "processed"
FIG = ROOT / "reports" / "figures"

RAW.mkdir(parents=True, exist_ok=True)
PROC.mkdir(parents=True, exist_ok=True)
FIG.mkdir(parents=True, exist_ok=True)


# =====================================================================
# EXCEPTION CUSTOM
# =====================================================================
class HardFailure(Exception):
    """Stoppe immédiatement le pipeline en cas d'erreur irréversible"""
    pass


# =====================================================================
# 1) GÉOCODAGE (Nominatim)
# =====================================================================
import requests
import time

def geocode_city(city: str):
    """Renvoie lat/lon pour une ville via Nominatim."""
    url = f"https://nominatim.openstreetmap.org/search?q={city}&format=json&limit=1"
    try:
        r = requests.get(url, headers={"User-Agent": "KayakApp"}, timeout=10)
        data = r.json()
        if len(data) == 0:
            return None, None
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None, None


def geocode_cities(cities):
    rows = []
    for c in cities:
        lat, lon = geocode_city(c)
        print(f"📍 {c:<25} → lat={lat}, lon={lon}")
        rows.append({"city": c, "lat": lat, "lon": lon})
        time.sleep(1)
    return pd.DataFrame(rows)


# =====================================================================
# 2) MÉTÉO (Open-Meteo)
# =====================================================================
def fetch_weather(df_geo, days=7):
    rows = []

    for _, row in df_geo.iterrows():
        city = row["city"]
        lat, lon = row["lat"], row["lon"]

        if pd.isna(lat) or pd.isna(lon):
            continue

        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&daily=temperature_2m_max,precipitation_sum&timezone=auto"
        )

        r = requests.get(url, timeout=10).json()

        temps = r["daily"]["temperature_2m_max"][:days]
        rains = r["daily"]["precipitation_sum"][:days]

        for t, rain in zip(temps, rains):
            rows.append({
                "city": city,
                "temp_day": t,
                "rain": rain,
            })

    return pd.DataFrame(rows)


# =====================================================================
# 3) SCORE DESTINATION
# =====================================================================
def compute_destination_score(temp_mean, rain_sum, price_mean):
    """
    Score global = Score météo (60%) + Score prix (40%)
    Score météo = normalisation temp + inverse pluie
    """

    if pd.isna(temp_mean) or pd.isna(rain_sum):
        return 0

    score_temp = (temp_mean - 5) / (30 - 5)
    score_temp = max(0, min(1, score_temp))

    score_rain = 1 - min(rain_sum / 50, 1)

    score_weather = 0.7 * score_temp + 0.3 * score_rain

    # score prix
    if pd.isna(price_mean):
        score_price = 0.5
    else:
        score_price = max(0, min(1, 1 - (price_mean - 50) / 150))

    final = 0.6 * score_weather + 0.4 * score_price
    return round(final, 4)


# =====================================================================
# 4) MAPS PLOTLY
# =====================================================================
def make_maps(df_geo, df_dest, df_hotels):
    # ---------------------------------
    # MAP 1 : DESTINATIONS
    # ---------------------------------
    fig1 = px.scatter_mapbox(
        df_dest,
        lat="lat", lon="lon",
        size="destination_score",
        color="destination_score",
        hover_name="city",
        color_continuous_scale="Turbo",
        zoom=5,
        height=800
    )
    fig1.update_layout(mapbox_style="open-street-map")
    fig1.write_html(FIG / "destinations_map.html")

    # ---------------------------------
    # MAP 2 : TOP 20 HÔTELS
    # ---------------------------------
    # Désactivation de la carte hôtels (lat/lon indisponibles)
    return

# =====================================================================
# 5) CHARGEMENT JSON (non utilisé dans nouvelle version LIVE)
# =====================================================================
def load_hotels_from_json(path: Path):
    if not path.exists():
        return pd.DataFrame(columns=["city", "hotelName", "score_num", "url", "lat", "lon", "price_eur"])

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return pd.DataFrame(data)


# =====================================================================
# 6) UPLOAD S3
# =====================================================================
def upload_file_to_s3(path: Path, s3_key: str):
    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        s3.upload_file(str(path), AWS_BUCKET, s3_key)

        print(f"☁️ Upload OK → s3://{AWS_BUCKET}/{s3_key}")

    except Exception as e:
        print(f"[ERR] Upload S3: {e}")



# =====================================================================
# 7) RDS
# =====================================================================
def load_to_rds(df_dest, df_hotels):
    try:
        print("🗄️ Connexion RDS…")

        engine = create_engine(
            RDS_URI,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10}
        )

        df_dest.to_sql("destinations", engine, if_exists="replace", index=False)
        df_hotels.to_sql("hotels", engine, if_exists="replace", index=False)

        print("🗄️ RDS OK")

    except Exception as e:
        print("❌ ERREUR RDS :")
        print(e)
        return


