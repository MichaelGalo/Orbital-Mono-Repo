import os
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

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


# ----------------- DONKI Section -----------------
st.header("Space Weather Database Of Notifications, Knowledge, Information (DONKI)")
donki_dataframe = get_dataset("DONKI", datasets["DONKI"])
st.dataframe(donki_dataframe)

# ----------------- Exoplanets Section -----------------
st.header("Exoplanets")
exoplanets_dataframe = get_dataset("Exoplanets", datasets["Exoplanets"])
st.dataframe(exoplanets_dataframe)
