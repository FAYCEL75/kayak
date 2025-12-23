import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
st.set_page_config(
    page_title="Kayak Destination Recommender",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------
# CSS LIGHT PREMIUM + FIX SELECTBOX BACKGROUND
# ---------------------------------------------------------
LIGHT_CSS = """
<style>

html, body {
    background-color: #e8edf5 !important;
    color: #111 !important;
}

/* MAIN WRAPPER */
.main, .block-container {
    background-color: #e8edf5 !important;
    padding-left: 2.5rem !important;
    padding-right: 2.5rem !important;
    max-width: 1500px !important;
    margin: auto !important;
}

/* GENERAL TEXT */
h1, h2, h3, h4, h5, p, div, span, label {
    color: #0f172a !important;
}

/* SIDEBAR DARK */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a, #142033, #1b2b44) !important;
    color: white !important;
}
section[data-testid="stSidebar"] * { color: white !important; }

/* 🔥 FIX DESTINATION SELECTBOX (fond blanc supprimé) */
/* SELECTBOX FULL KAYAK STYLE */
div[data-baseweb="select"] > div {
    background-color: #ff6f00 !important; /* bleu foncé = aucun blanc */
    color: #ffffff !important;
    border: 1px solid rgba(255,255,255,0.35) !important;
}

/* Texte sélectionné */
div[data-baseweb="select"] div[role="button"] {
    color: #ffffff !important;
}

/* Placeholder + input interne */
div[data-baseweb="select"] input {
    color: #ffffff !important;
    caret-color: #ffffff !important;
}

/* Liste déroulante */
ul[role="listbox"] {
    background-color: #233455 !important;
}
ul[role="listbox"] li {
    color: #ffffff !important;
}
ul[role="listbox"] li:hover {
    background-color: #FF6F00 !important;
    color: #111 !important;
}
}

/* DATAFRAME */
div[data-testid="stDataFrame"] {
    background-color: #f1f5f9 !important;
    border: 1px solid #cbd5e1 !important;
    color: #0f172a !important;
    border-radius: 8px;
}

/* HEADER */
.main-header {
    background: linear-gradient(90deg, #ff6f00, #ff9140);
    padding: 1.2rem 2rem;
    border-radius: 1rem;
    color: #fff !important;
    display: flex;
    align-items: center;
    margin-bottom: 1rem;
}
.kayak-letter {
    background-color: #111;
    color: #ff6f00 !important;
    padding: 5px 9px;
    font-weight: 900;
    border-radius: 4px;
    margin-right: 3px;
    font-size: 1.4rem;
}
</style>
"""
st.markdown(LIGHT_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA_PROC = ROOT / "reports" / "processed"

DEST_PATH = DATA_PROC / "destinations_score.csv"
HOTELS_PATH = DATA_PROC / "hotels_clean.csv"

@st.cache_data
def load_data():
    return pd.read_csv(DEST_PATH), pd.read_csv(HOTELS_PATH)

df_dest, df_hotels = load_data()

# ---------------------------------------------------------
# SIDEBAR – SCORING
# ---------------------------------------------------------
st.sidebar.header("⚙️ Pondération du scoring")

w_meteo = st.sidebar.slider("Poids météo (%)", 0, 100, 60, 5)
w_prix = st.sidebar.slider("Poids prix (%)", 0, 100, 25, 5)
w_hotel = st.sidebar.slider("Poids qualité hôtels (%)", 0, 100, 15, 5)

total = w_meteo + w_prix + w_hotel
w_meteo /= total
w_prix /= total
w_hotel /= total

# ---------------------------------------------------------
# SCORING
# ---------------------------------------------------------
df_dest["score_temp"] = ((df_dest["temp_mean"] - 5) / 25).clip(0, 1)
df_dest["score_rain"] = (1 - df_dest["rain_sum"] / 50).clip(0, 1)
df_dest["score_weather"] = 0.7 * df_dest["score_temp"] + 0.3 * df_dest["score_rain"]

df_dest["score_price"] = (1 - ((df_dest["price_mean"] - 50) / 150)).clip(0, 1).fillna(0.5)
df_dest["score_review"] = ((df_dest["score_mean"] - 6) / 3.5).clip(0, 1).fillna(0.5)

df_dest["destination_score"] = (
    w_meteo * df_dest["score_weather"] +
    w_prix * df_dest["score_price"] +
    w_hotel * df_dest["score_review"]
)

df_dest["score_norm_100"] = 100 * (
    (df_dest["destination_score"] - df_dest["destination_score"].min()) /
    (df_dest["destination_score"].max() - df_dest["destination_score"].min())
)

df_dest = df_dest.sort_values("destination_score", ascending=False).reset_index(drop=True)
df_dest["rank"] = df_dest.index + 1
cities = df_dest["city"].tolist()

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------
st.markdown("""
<div class="main-header">
    <span class="kayak-letter">K</span>
    <span class="kayak-letter">A</span>
    <span class="kayak-letter">Y</span>
    <span class="kayak-letter">A</span>
    <span class="kayak-letter">K</span>
    <h1 style="margin-left: 1rem;">Destination Recommender</h1>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# SIDEBAR – FILTERS
# ---------------------------------------------------------
st.sidebar.header("🎯 Filtres")

selected_city = st.sidebar.selectbox("Destination", ["Toutes les destinations"] + cities)
top_n_hotels = st.sidebar.slider("Nombre d'hôtels à afficher", 5, 30, 10, 5)

# ---------------------------------------------------------
# GLOBAL VIEW (MAP + TOP 10)
# ---------------------------------------------------------
st.subheader("🌍 Vue globale des destinations")

col_map, col_table = st.columns([2, 1])

with col_map:
    fig_map = px.scatter_mapbox(
        df_dest,
        lat="lat", lon="lon",
        size="score_norm_100",
        color="score_norm_100",
        color_continuous_scale="Turbo",
        zoom=5, height=600
    )
    fig_map.update_layout(mapbox_style="open-street-map", margin=dict(l=0,r=0,t=0,b=0))
    st.plotly_chart(fig_map, use_container_width=True)

with col_table:
    st.markdown("**🏆 Top 10 destinations**")
    st.dataframe(
        df_dest[["rank","city","score_norm_100","temp_mean","rain_sum","price_mean"]].head(10),
        use_container_width=True, hide_index=True
    )

# ---------------------------------------------------------
# DESTINATION DETAILS
# ---------------------------------------------------------
st.subheader("🔍 Détail destination & hôtels")

if selected_city != "Toutes les destinations":
    row = df_dest[df_dest["city"] == selected_city].iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rang", int(row["rank"]))
    c2.metric("Score", f"{row['score_norm_100']:.1f}/100")
    c3.metric("Temp. moy", f"{row['temp_mean']:.1f}°C")
    c4.metric("Pluie 7j", f"{row['rain_sum']:.1f} mm")

    df_hot_city = df_hotels[df_hotels["city"] == selected_city].copy()
    df_hot_city = df_hot_city.sort_values("score", ascending=False).head(top_n_hotels)

    st.markdown("### 🏨 Hôtels recommandés")
    st.dataframe(df_hot_city[["hotelName","score","price_eur","url"]], hide_index=True)
else:
    st.info("Sélectionner une destination pour afficher les détails.")

# ---------------------------------------------------------
# ⭐ TOP 5 MEILLEURES DESTINATIONS RECOMMANDÉES
# ---------------------------------------------------------
st.subheader("🏆 Top 5 destinations recommandées")

st.dataframe(
    df_dest[["rank","city","score_norm_100","temp_mean","rain_sum","price_mean"]].head(5),
    use_container_width=True, hide_index=True
)

# ---------------------------------------------------------
# 🔥 COMPARAISON ENTRE 2 VILLES
# ---------------------------------------------------------
st.subheader("🆚 Comparateur de 2 destinations")

colA, colB = st.columns(2)
with colA:
    compA = st.selectbox("Destination A", cities, key="cmpA")
with colB:
    compB = st.selectbox("Destination B", cities, key="cmpB")

df_comp = df_dest[df_dest["city"].isin([compA, compB])]
st.dataframe(
    df_comp[["city","score_norm_100","temp_mean","rain_sum","price_mean"]],
    use_container_width=True, hide_index=True
)

# ---------------------------------------------------------
# SCORING EXPLANATION
# ---------------------------------------------------------
with st.expander("ℹ️ Comment fonctionne le scoring ?"):
    st.markdown(f"""
    ### 🧠 Explication du scoring dynamique

    #### Poids actuels
    - **Météo : {w_meteo*100:.0f}%**
    - **Prix des hôtels : {w_prix*100:.0f}%**
    - **Qualité des hôtels : {w_hotel*100:.0f}%**

    ### 1) 🌤️ Score météo  
    Basé sur température + pluie cumulée.

    ### 2) 💶 Score prix  
    Prix moyen → plus c’est cher, plus on pénalise.
    
    
    PS : 
    
    Si Booking ne renvoie pas les prix, la valeur est None.
    
    J’ai choisi de mettre un score prix neutre (0.5), donc ni bonus, ni malus, ce qui évite de pénaliser une destination juste à cause d’un manque de données côté Booking.

    ### 3) 🏨 Score qualité hôtels  
    Note Booking moyenne.

    ### 4) 🧮 Score final  
    ```python
    destination_score =
          poids_météo × score_météo
        + poids_prix × score_prix
        + poids_hôtels × score_review
    ```
    Normalisation finale : 0 → 100.
    """)
# ---------------------------------------------------------
# SOURCES
# ---------------------------------------------------------
st.subheader("📂 Sources des données & infrastructure")

st.markdown("""
| Élément | Source / Infra |
|--------|-----------------|
| 🌍 **Géocodage** | OpenStreetMap — Nominatim |
| ⛅ **Météo** | Open-Meteo |
| 🏨 **Hôtels** | Booking.com (Scraping Selenium optimisé) |
| ☁️ **Stockage** | AWS S3 — `jedha-kayak-datalake` |
| 🗄️ **Base SQL** | AWS RDS PostgreSQL |
| 🔥 **Code ETL** | `src/etl.py` |
""")