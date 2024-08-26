import streamlit as st
from PIL import Image


st.set_page_config(
        page_title="LOTUS AI",
        page_icon="🧠",
        initial_sidebar_state="collapsed"
)

st.title("Lotus AI Streamlit Ödevi")
st.text("Eren GÜNER"
        "\nKonya Teknik Üniversitesi"
        "\nYazılım Mühendisliği 4. Sınıf")

st.sidebar.success("Detaylı bilgi almak için sayfa seçiniz")

st.subheader("Dask, Vaex, Polars, Modin, Koalas ve Pyspark Kütüphanelerinin Karşılaştırılması")

img = Image.open("tablo.png")
st.image(img, caption="Karşılaştırma Tablosu")
