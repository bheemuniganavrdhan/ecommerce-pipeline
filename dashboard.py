import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import time

st.set_page_config(page_title="E-Commerce Stream Monitor", layout="wide")

DB_CONN_STR = "postgresql+psycopg://admin:adminpassword@localhost:5432/ecommerce_dw"

@st.cache_resource
def get_db_engine():
    return create_engine(DB_CONN_STR)

def load_data():
    try:
        engine = get_db_engine()
        query = """
        SELECT window_start, window_end, product_category, total_gmv, cart_additions, completed_purchases
        FROM hourly_category_metrics
        ORDER BY window_end DESC
        LIMIT 100;
        """
        return pd.read_sql(query, con=engine)
    except Exception as e:
        return pd.DataFrame()

st.title("🛒 Large-Scale E-Commerce Telemetry Pipeline")
st.caption("Live streaming analytics powered by Kafka & PostgreSQL")

# Auto-refresh checkbox
auto_refresh = st.sidebar.checkbox("Auto Refresh (every 3s)", value=True)

df = load_data()

if df.empty:
    st.info("⏳ Waiting for stream records... Producer & Stream Processor run avthunnaya chusuko!")
else:
    total_revenue = df["total_gmv"].sum()
    total_purchases = df["completed_purchases"].sum()
    total_carts = df["cart_additions"].sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Observed Revenue (GMV)", f"${total_revenue:,.2f}")
    c2.metric("Orders Placed", f"{total_purchases:,}")
    c3.metric("Cart Additions", f"{total_carts:,}")

    st.markdown("---")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Revenue by Category")
        cat_summary = df.groupby("product_category")["total_gmv"].sum().reset_index()
        fig_bar = px.bar(cat_summary, x="product_category", y="total_gmv", color="product_category",
                         labels={"total_gmv": "Gross Merchandise Value ($)"})
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_b:
        st.subheader("Real-time Ingested Stream")
        st.dataframe(df.head(10), use_container_width=True)

if auto_refresh:
    time.sleep(3)
    st.rerun()