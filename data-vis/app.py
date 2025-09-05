import os
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
load_dotenv()

endpoints = {
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


st.title("Basic Data Dashboard")


# -- Astronauts Section --
st.header("Astronauts")
astro_dataframe = get_dataset("Astronauts", endpoints["Astronauts"])
st.dataframe(astro_dataframe)

# -- APOD Section --
st.header("Astronomy Picture of the Day (APOD)")
apod_dataframe = get_dataset("APOD", endpoints["APOD"])
st.dataframe(apod_dataframe)

# -- DONKI Section --
st.header("Space Weather Database Of Notifications, Knowledge, Information (DONKI)")
donki_dataframe = get_dataset("DONKI", endpoints["DONKI"])
st.dataframe(donki_dataframe)

# -- Exoplanets Section --
st.header("Exoplanets")
exoplanets_dataframe = get_dataset("Exoplanets", endpoints["Exoplanets"])
st.dataframe(exoplanets_dataframe)
