import os
import requests
import pandas as pd
import streamlit as st
import numpy as np
from dotenv import load_dotenv

load_dotenv()

datasets = {
    "Astronauts": os.getenv("API_ASTRONAUTS_DATASET"),
    "APOD": os.getenv("API_APOD_DATASET"),
    "DONKI": os.getenv("API_DONKI_DATASET"),
    "Exoplanets": os.getenv("API_EXOPLANETS_DATASET"),
}

@st.cache_data
def get_dataset(name, url):    
    try:
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()      
        dataframe = pd.DataFrame(data)
        return dataframe
    except Exception as e:
        st.error(f"Failed to load {name}: {e}")

st.set_page_config(page_title="Rapid Dashboard", layout="wide")
st.title("Rapid Dashboard")

# ----------------- Astronauts Section -----------------
st.header("Astronauts")
astro_dataframe = get_dataset("Astronauts", datasets["Astronauts"])

# conversions
astro_dataframe["age"] = pd.to_numeric(astro_dataframe["age"], errors="coerce")
astro_dataframe["eva_time"] = pd.to_numeric(astro_dataframe["eva_time"], errors="coerce")
astro_dataframe["spacewalks_count"] = pd.to_numeric(astro_dataframe["spacewalks_count"], errors="coerce")
astro_dataframe["agency_abbrev"] = pd.to_numeric(astro_dataframe["agency_abbrev"], errors="coerce")

# aggregation
total_astronauts = len(astro_dataframe)
average_age = round(astro_dataframe["age"].mean())
total_spacewalks = astro_dataframe["spacewalks_count"].sum()
number_of_agencies = astro_dataframe["agency"].nunique()

# display
col1, col2, col3, col4 = st.columns(4)
col1.metric("Astronauts In Space", total_astronauts)
col2.metric("Unique Space Agencies", number_of_agencies)
col3.metric("Average Age", average_age)
col4.metric("Total Spacewalks", total_spacewalks)

st.subheader("Top Agencies")
agency_counts = astro_dataframe["agency"].value_counts()
st.bar_chart(agency_counts)


# ----------------- APOD Section -----------------
st.header("Astronomy Picture of the Day (APOD)")
apod_dataframe = get_dataset("APOD", datasets["APOD"])
if apod_dataframe is None or apod_dataframe.empty:
    st.warning("No APOD data available.")
else:
    selected_row = apod_dataframe.iloc[0]

    if "title" in apod_dataframe.columns:
        caption_text = selected_row.get("title", "Untitled")
    elif "date" in apod_dataframe.columns:
        caption_text = selected_row.get("date", "Untitled")
    else:
        caption_text = None

    if "url" in apod_dataframe.columns and selected_row.get("url"):
        st.image(str(selected_row.get("url")), caption=caption_text, use_container_width=True)
    else:
        st.info("Current media type not supported")
        st.markdown("Visit the [APOD website](https://apod.nasa.gov/apod/astropix.html) for more details.")

    if "explanation" in apod_dataframe.columns:
        st.markdown("**Description**")
        st.write(selected_row.get("explanation"))
    elif "description" in apod_dataframe.columns:
        st.markdown("**Description**")
        st.write(selected_row.get("description"))


# ----------------- DONKI Analysis -----------------
st.header("Space Weather Alerts")
st.markdown("This section analyzes space weather alerts from NASA's DONKI (Space Weather Database Of Notifications, Knowledge, Information).")

donki_dataframe = get_dataset("DONKI", datasets["DONKI"])

if donki_dataframe is not None and not donki_dataframe.empty:
    message_type_mapping = {
        "CME": "Coronal Mass Ejection",
        "CMEAnalysis": "Coronal Mass Ejection Analysis",
        "GST": "Geomagnetic Storm",
        "IPS": "Interplanetary Shock",
        "FLR": "Solar Flare",
        "SPE": "Solar Particle Event",
        "MPC": "Magnetopause Crossing",
        "RBE": "Radiation Belt Enhancement",
        "HSS": "High Speed Stream",
        "Notifications": "Notifications"
    }

    donki_dataframe['message_type_name'] = donki_dataframe['message_type'].map(message_type_mapping)
    type_counts = donki_dataframe['message_type_name'].value_counts()

    total_alerts = len(donki_dataframe)
    st.metric("Total Alerts (5-year period)", total_alerts)

    st.subheader("Number of Messages by Type")
    st.bar_chart(type_counts)

else:
    st.warning("No DONKI data available for analysis.")


# ----------------- Exoplanets Section -----------------
st.header("Exoplanets Overview")
exoplanets_dataframe = get_dataset("Exoplanets", datasets["Exoplanets"])

if exoplanets_dataframe is not None and not exoplanets_dataframe.empty:
    # --- Glance Metrics ---
    total_exoplanets = len(exoplanets_dataframe)
    avg_distance = round(exoplanets_dataframe["system_distance"].mean(), 2)
    unique_host_stars = exoplanets_dataframe["host_star"].nunique()
    controversial_count = exoplanets_dataframe["controversial_flag"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Exoplanets", total_exoplanets)
    col2.metric("Average Distance to Host Star (ly)", avg_distance)
    col3.metric("Number of Host Stars", unique_host_stars)
    col4.metric("Controversial Discoveries", controversial_count)

    st.markdown("---")

    # --- Histograms / Charts ---
    st.subheader("Exoplanet Discoveries by Year")
    discovery_year_counts = exoplanets_dataframe["discovery_year"].value_counts().sort_index()
    st.bar_chart(discovery_year_counts)

    # --- Planets by Discovery Method ---
    st.subheader("Planets by Discovery Method")
    method_counts = exoplanets_dataframe["discovery_method"].value_counts()
    st.bar_chart(method_counts)

    # --- Exoplanet Radius Compared to Earth ---
    st.subheader("Exoplanet Radius Compared to Earth")
    radii = exoplanets_dataframe["radius_earth_radii"].dropna()
    max_radius = min(radii.max(), 35)
    bins = np.arange(0, max_radius + 0.5, 0.5)
    counts, bin_edges = np.histogram(radii, bins=bins)
    labels = [f"{bin_edges[i]:.1f}–{bin_edges[i+1]:.1f}" for i in range(len(bin_edges)-1)]
    radius_df = pd.DataFrame({"Bin": pd.Categorical(labels, categories=labels, ordered=True), "Count": counts})
    st.bar_chart(radius_df.set_index("Bin"))


    # --- Host Star Radius Compared to Sun ---
    st.subheader("Host Star Ratio Compared to Sun")
    star_radii = exoplanets_dataframe["star_radius_solar_radii"].dropna()
    max_star_radius = min(star_radii.max(), 10)
    bins = np.arange(0, max_star_radius + 0.5, 0.5)
    counts, bin_edges = np.histogram(star_radii, bins=bins)
    labels = [f"{bin_edges[i]:.1f}–{bin_edges[i+1]:.1f}" for i in range(len(bin_edges)-1)]

    star_radius_df = pd.DataFrame({
        "Bin": labels,
        "Count": counts
    })
    st.bar_chart(star_radius_df.set_index("Bin"))

else:
    st.warning("No exoplanets data available.")
