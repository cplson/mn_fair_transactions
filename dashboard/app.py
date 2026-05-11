"""MN Fair Sales Dashboard (Streamlit).

Connectivity, KPIs, schema explorer, and charts read `fair.db` in read-only
mode. Filters and refresh polish land in later checkpoints.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Streamlit puts `dashboard/` on sys.path, not the repo root, so absolute
# imports like `from dashboard.db import ...` fail unless the project root
# is prepended (local `streamlit run dashboard/app.py` and Docker both).
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_root = str(_PROJECT_ROOT)
if _root not in sys.path:
    sys.path.insert(0, _root)

import sqlite3

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.db import DB_PATH, run_query
from dashboard import queries

st.set_page_config(
    page_title="MN Fair Sales Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("MN Fair Sales Dashboard")
st.caption(
    "Star schema: `fact_sales` joined to `dim_vendor`, `dim_product`, `dim_time` "
    "(SQLite, read-only)."
)

st.markdown(
    """
    **Pipeline at a glance**

    1. `generate_transactions.py` writes OLTP rows into `transactions`.
    2. The Airflow DAG `mn_fair_elt` extracts new rows to `staging/` and
       loads them into the OLAP star schema.
    3. This dashboard reads the star schema in `mode=ro` and never writes.
    """
)

st.subheader("Checkpoint 3: KPIs, explorer, and charts")

st.write(f"Database: `{DB_PATH}`")

try:
    total_revenue = float(run_query(queries.KPI_TOTAL_REVENUE).iloc[0]["total_revenue"])
    total_transactions = int(
        run_query(queries.KPI_TOTAL_TRANSACTIONS).iloc[0]["total_transactions"]
    )
    average_ticket = float(run_query(queries.KPI_AVERAGE_TICKET).iloc[0]["average_ticket"])
    active_vendors = int(run_query(queries.KPI_ACTIVE_VENDORS).iloc[0]["active_vendors"])
    row_counts_df = run_query(queries.ROW_COUNTS)
    joined_sample_df = run_query(queries.STAR_SCHEMA_SAMPLE)
    revenue_time_df = run_query(queries.CHART_REVENUE_OVER_TIME)
    top_vendors_df = run_query(queries.CHART_TOP_VENDORS)
    top_products_df = run_query(queries.CHART_TOP_PRODUCTS)
    vendor_product_df = run_query(queries.CHART_VENDOR_PRODUCT_REVENUE)
except sqlite3.OperationalError as exc:
    if "no such table" in str(exc).lower():
        st.warning(
            "Connected to `fair.db`, but `fact_sales` doesn't exist yet. "
            "Run the OLAP load at least once: "
            "`docker compose run --rm pipeline python scripts/load_to_olap.py`."
        )
    else:
        raise
except FileNotFoundError as exc:
    st.error(str(exc))
else:
    st.success("Connected to `fair.db` and loaded star schema metrics.")

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    kpi_col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    kpi_col2.metric("Total Transactions", f"{total_transactions:,}")
    kpi_col3.metric("Average Ticket", f"${average_ticket:,.2f}")
    kpi_col4.metric("Active Vendors", f"{active_vendors:,}")

    st.divider()
    st.subheader("Star schema explorer")

    st.caption("Row counts per dimension and fact table.")
    st.dataframe(row_counts_df, use_container_width=True, hide_index=True)

    st.caption("Recent joined sample (`fact_sales` + `dim_time` + `dim_vendor` + `dim_product`).")
    st.dataframe(joined_sample_df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Charts (full star schema, no filters yet)")

    # --- Revenue over time ---
    st.markdown("**Revenue by hour** (`fact_sales` → `dim_time`)")
    with st.expander("Show SQL"):
        st.code(queries.CHART_REVENUE_OVER_TIME.strip(), language="sql")
    if revenue_time_df.empty:
        st.info("No rows in `fact_sales` yet — nothing to plot.")
    else:
        line_df = revenue_time_df.copy()
        line_df["period"] = pd.to_datetime(
            line_df["period"], format="%Y-%m-%d %H:%M", errors="coerce"
        )
        line_df = line_df.dropna(subset=["period"]).set_index("period")[["revenue"]]
        if line_df.empty:
            st.warning("Could not parse time buckets for the line chart; check `dim_time` data.")
        else:
            st.line_chart(line_df)

    # --- Top vendors ---
    st.markdown("**Top vendors by revenue** (`fact_sales` → `dim_vendor`)")
    with st.expander("Show SQL"):
        st.code(queries.CHART_TOP_VENDORS.strip(), language="sql")
    if top_vendors_df.empty:
        st.info("No rows in `fact_sales` yet — nothing to plot.")
    else:
        vendor_chart = (
            alt.Chart(top_vendors_df)
            .mark_bar()
            .encode(
                x=alt.X("revenue:Q", title="Revenue ($)"),
                y=alt.Y("vendor_name:N", sort="-x", title="Vendor"),
                tooltip=["vendor_name", "revenue"],
            )
            .properties(height=220)
        )
        st.altair_chart(vendor_chart, use_container_width=True)

    # --- Top products ---
    st.markdown("**Top products by revenue** (`fact_sales` → `dim_product`)")
    with st.expander("Show SQL"):
        st.code(queries.CHART_TOP_PRODUCTS.strip(), language="sql")
    if top_products_df.empty:
        st.info("No rows in `fact_sales` yet — nothing to plot.")
    else:
        product_chart = (
            alt.Chart(top_products_df)
            .mark_bar()
            .encode(
                x=alt.X("revenue:Q", title="Revenue ($)"),
                y=alt.Y("product_name:N", sort="-x", title="Product"),
                tooltip=["product_name", "revenue"],
            )
            .properties(height=220)
        )
        st.altair_chart(product_chart, use_container_width=True)

    # --- Vendor × product heatmap ---
    st.markdown("**Vendor × product revenue** (all three dimensions)")
    with st.expander("Show SQL"):
        st.code(queries.CHART_VENDOR_PRODUCT_REVENUE.strip(), language="sql")
    if vendor_product_df.empty:
        st.info("No rows in `fact_sales` yet — nothing to plot.")
    else:
        heat = (
            alt.Chart(vendor_product_df)
            .mark_rect()
            .encode(
                x=alt.X("product_name:N", title="Product", sort=None),
                y=alt.Y("vendor_name:N", title="Vendor", sort=None),
                color=alt.Color("revenue:Q", title="Revenue ($)"),
                tooltip=["vendor_name", "product_name", "revenue"],
            )
            .properties(height=260)
        )
        st.altair_chart(heat, use_container_width=True)
