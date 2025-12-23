# Bloc 1 — Kayak (France Top-35) — **STRICT REAL DATA**

Ce projet suit **à la lettre** le brief :
- **Scrape data from destinations** → Nominatim pour géocoder les 35 villes
- **Get weather data** → OpenWeather One Call (7 jours) — nécessite **OWM_API_KEY**
- **Get hotels' info** → Booking.com (≈15 hôtels/ville), avec **géolocalisation réelle** (lat/lon)
- **Store in a data lake** → CSV bruts dans `data/raw/`
- **ETL vers data warehouse** → tables propres + `mart_city_summary.csv`

## Exécution
```bash
cd bloc1_kayak_infra
python -m venv .venv && source .venv/bin/activate   # Win: .venv\Scripts\Activate.ps1
pip install -r requirements.txt

export OWM_API_KEY=VOTRE_CLE_OPENWEATHER    # OBLIGATOIRE
python src/etl.py
```

**Aucune donnée synthétique** : si Internet est indisponible ou si l'API manque, le script **s'arrête**.

## Données
- **Raw (Data Lake)** : `cities_scope.csv`, `geocoding.csv`, `weather_raw.csv`, `hotels_raw.csv`
- **Processed (DW)** : `dim_destination.csv`, `fact_weather.csv`, `dim_hotel.csv`, `mart_city_summary.csv`

## Sorties visualisations
- `reports/figures/top5_destinations_map.html` — Top-5 destinations (réel)
- `reports/figures/top20_hotels_map.html` — Top-20 hôtels géolocalisés (réel)

## Bonnes pratiques scraping
- **User-Agent** explicite
- **Pauses** entre requêtes (1–2 s)
- Volume raisonnable (~15 hôtels * 35 villes ≈ 525 entrées)

## (Optionnel) AWS
- Export CSV vers **S3** et chargement dans **RDS** possibles (helpers dans `src/aws_io.py` si besoin).

