import pandas as pd
import plotly.express as px
import streamlit as st
from db import get_engine

st.set_page_config(page_title="Market Pipeline Dashboard", layout="wide")
st.title("📊 Market Pipeline — Actions & Crypto")

engine = get_engine()

@st.cache_data(ttl=60)
def load_stocks():
    return pd.read_sql("SELECT * FROM latest_stock_prices ORDER BY ticker", engine)

@st.cache_data(ttl=60)
def load_crypto():
    return pd.read_sql("SELECT * FROM latest_crypto_prices ORDER BY coin", engine)

tab1, tab2, tab3 = st.tabs(["Actions", "Crypto", "Santé du pipeline"])

with tab1:
    df_stocks = load_stocks()
    st.subheader("Derniers prix — Actions")
    st.dataframe(df_stocks, use_container_width=True)

    fig = px.bar(df_stocks, x="ticker", y="close_price", title="Prix de clôture par ticker")
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    df_crypto = load_crypto()
    st.subheader("Derniers prix — Crypto")
    st.dataframe(df_crypto, use_container_width=True)

    fig2 = px.bar(df_crypto, x="coin", y="price_usd", title="Prix par cryptomonnaie")
    st.plotly_chart(fig2, use_container_width=True)

    fig3 = px.bar(
        df_crypto, x="coin", y="change_24h_pct",
        title="Variation 24h (%)",
        color="change_24h_pct",
        color_continuous_scale=["red", "green"],
    )
    st.plotly_chart(fig3, use_container_width=True)

with tab3:
    st.subheader("Santé du pipeline")

    @st.cache_data(ttl=30)
    def load_pipeline_runs():
        return pd.read_sql(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 20",
            engine
        )

    @st.cache_data(ttl=30)
    def load_freshness():
        return pd.read_sql("""
            SELECT 'Actions' AS source, MAX(fetched_at) AS derniere_maj
            FROM raw_stock_prices
            UNION ALL
            SELECT 'Crypto' AS source, MAX(fetched_at) AS derniere_maj
            FROM raw_crypto_prices
        """, engine)

    freshness = load_freshness()
    freshness["minutes_depuis_maj"] = (
        pd.Timestamp.now(tz="UTC") - freshness["derniere_maj"]
    ).dt.total_seconds() / 60

    col1, col2 = st.columns(2)
    for i, row in freshness.iterrows():
        col = col1 if i == 0 else col2
        alerte = "🔴" if row["minutes_depuis_maj"] > 30 else "🟢"
        col.metric(
            label=f"{alerte} {row['source']} — dernière mise à jour",
            value=f"{row['minutes_depuis_maj']:.0f} min",
        )

    st.subheader("Derniers runs")
    runs = load_pipeline_runs()
    st.dataframe(runs, use_container_width=True)