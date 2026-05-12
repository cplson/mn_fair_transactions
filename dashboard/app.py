"""MN Fair Sales Dashboard (Streamlit) — presentation build.

Loads read-only sales metrics for the UI. Filter and chart logic live in
`dashboard.queries`; this module keeps audience-facing copy free of schema
and SQL jargon.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
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

from dashboard.db import run_query
from dashboard import queries


def _looks_like_corrupt_sqlite(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return "malformed" in text or "disk image is malformed" in text


_CORRUPT_STORE_HELP = """
The on-disk sales file could not be read. That usually means it was damaged—
for example two programs wrote at the same time, or the machine stopped mid-save.

**Fix (pick the environment you use):**

- **On your machine:** from the project root, run `./scripts/reset.sh`, then run your usual **load** step once so history is rebuilt, and refresh this page.
- **Docker pipeline:**  
  `docker compose run --rm pipeline bash -c './scripts/reset.sh && python scripts/load_to_olap.py'`

Before resetting, stop anything that keeps appending sales (simulators, schedulers) so the new file is not torn again.
""".strip()


st.set_page_config(
    page_title="Minnesota State Fair — Sales",
    page_icon=":corn:",
    layout="wide",
)

st.title("Minnesota State Fair — Sales snapshot")
st.caption(
    "Live-style view of snack and drink sales across vendors. "
    "Use the sidebar to focus on dates, vendors, or menu items."
)

st.markdown(
    """
    **How to read this page**

    1. Sales are recorded as guests buy food and drinks from vendors.
    2. A scheduled job keeps the numbers up to date from the operational system.
    3. What you see here is read-only: filters change the view, not the source records.
    """
)

st.subheader("Summary")

try:
    bounds = run_query(queries.DATE_BOUNDS_DIM_TIME)
    vendors_df = run_query(queries.DIM_VENDOR_OPTIONS)
    products_df = run_query(queries.DIM_PRODUCT_OPTIONS)
    row_counts_df = run_query(queries.ROW_COUNTS)
except Exception as exc:
    if _looks_like_corrupt_sqlite(exc):
        run_query.clear()
        st.error(_CORRUPT_STORE_HELP)
        st.stop()
    if isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower():
        st.warning(
            "Sales history is not available yet. "
            "After the first load job completes, refresh this page."
        )
        st.stop()
    if isinstance(exc, FileNotFoundError):
        st.error(
            "No local sales file was found. "
            "Run the project setup so the data folder is created, then try again."
        )
        st.stop()
    raise

row0 = bounds.iloc[0]
if pd.isna(row0["dmin"]) or pd.isna(row0["dmax"]):
    dmin = dmax = date.today()
else:
    dmin = pd.to_datetime(row0["dmin"]).date()
    dmax = pd.to_datetime(row0["dmax"]).date()
if dmin > dmax:
    dmin, dmax = dmax, dmin

vendor_names = vendors_df["vendor_name"].tolist()
vendor_id_by_name = dict(zip(vendors_df["vendor_name"], vendors_df["vendor_id"]))
product_names = products_df["product_name"].tolist()
product_id_by_name = dict(zip(products_df["product_name"], products_df["product_id"]))

with st.sidebar:
    st.header("Focus")
    date_range = st.date_input(
        "Date range",
        value=(dmin, dmax),
        min_value=dmin,
        max_value=dmax,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d_start, d_end = date_range[0], date_range[1]
    elif hasattr(date_range, "isoformat"):
        d_start = d_end = date_range
    else:
        d_start, d_end = dmin, dmax
    if d_start > d_end:
        d_start, d_end = d_end, d_start
    date_start_s = d_start.isoformat()
    date_end_s = d_end.isoformat()

    selected_vendors = st.multiselect(
        "Vendors (leave empty for all)",
        options=vendor_names,
        default=vendor_names,
    )
    selected_products = st.multiselect(
        "Menu items (leave empty for all)",
        options=product_names,
        default=product_names,
    )

vendor_ids = tuple(vendor_id_by_name[n] for n in selected_vendors) if selected_vendors else ()
product_ids = tuple(product_id_by_name[n] for n in selected_products) if selected_products else ()

refreshed_at = datetime.now()
st.caption(
    f"View generated **{refreshed_at:%Y-%m-%d %H:%M:%S}** — figures may lag live sales by up to **15 seconds**."
)

try:
    total_revenue = float(
        run_query(
            *queries.kpi_total_revenue(date_start_s, date_end_s, vendor_ids, product_ids)
        ).iloc[0]["total_revenue"]
    )
    total_transactions = int(
        run_query(
            *queries.kpi_total_transactions(date_start_s, date_end_s, vendor_ids, product_ids)
        ).iloc[0]["total_transactions"]
    )
    average_ticket = float(
        run_query(
            *queries.kpi_average_ticket(date_start_s, date_end_s, vendor_ids, product_ids)
        ).iloc[0]["average_ticket"]
    )
    active_vendors = int(
        run_query(
            *queries.kpi_active_vendors(date_start_s, date_end_s, vendor_ids, product_ids)
        ).iloc[0]["active_vendors"]
    )
    joined_sample_df = run_query(
        *queries.star_schema_sample(date_start_s, date_end_s, vendor_ids, product_ids)
    )
    revenue_time_df = run_query(
        *queries.chart_revenue_over_time(date_start_s, date_end_s, vendor_ids, product_ids)
    )
    top_vendors_df = run_query(
        *queries.chart_top_vendors(date_start_s, date_end_s, vendor_ids, product_ids)
    )
    top_products_df = run_query(
        *queries.chart_top_products(date_start_s, date_end_s, vendor_ids, product_ids)
    )
    vendor_product_df = run_query(
        *queries.chart_vendor_product_revenue(date_start_s, date_end_s, vendor_ids, product_ids)
    )
except Exception as exc:
    if _looks_like_corrupt_sqlite(exc):
        run_query.clear()
        st.error(_CORRUPT_STORE_HELP)
        st.stop()
    if isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower():
        st.warning(
            "Detailed sales are not loaded yet. "
            "Run the load step once, then refresh this page."
        )
        st.stop()
    if isinstance(exc, FileNotFoundError):
        st.error(
            "No local sales file was found. "
            "Run the project setup so the data folder and sales file are created, then try again."
        )
        st.stop()
    raise

st.success("Showing sales for your current filters.")

kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
kpi_col1.metric("Total revenue", f"${total_revenue:,.2f}")
kpi_col2.metric("Transactions", f"{total_transactions:,}")
kpi_col3.metric("Average ticket", f"${average_ticket:,.2f}")
kpi_col4.metric("Vendors with sales", f"{active_vendors:,}")

st.divider()
st.subheader("Behind the scenes")

st.caption(
    "Rough inventory of how much history exists overall (not narrowed by your filters). "
    "The headline metrics and charts below follow your sidebar choices."
)
_catalog_labels = {
    "dim_vendor": "Registered vendors",
    "dim_product": "Menu items in catalog",
    "dim_time": "Distinct clock times in history",
    "fact_sales": "Individual sale lines",
}
row_display = row_counts_df.copy()
row_display["Area"] = row_display["table_name"].map(_catalog_labels).fillna(
    row_display["table_name"]
)
row_display = row_display.rename(columns={"row_count": "Rows"})[["Area", "Rows"]]
st.dataframe(row_display, use_container_width=True, hide_index=True)

st.caption("Recent sales matching your filters (newest first, up to twenty lines).")
_join_labels = {
    "transaction_id": "Receipt #",
    "timestamp": "When",
    "vendor_name": "Vendor",
    "product_name": "Item",
    "quantity": "Qty",
    "amount": "Line total ($)",
}
joined_display = joined_sample_df.rename(columns=_join_labels)
st.dataframe(joined_display, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Charts")

# --- Revenue over time ---
st.markdown("**Revenue by day**")
if revenue_time_df.empty:
    st.info("Nothing to plot for this combination — try widening the date range or clearing filters.")
else:
    line_df = revenue_time_df.copy()
    line_df["period"] = pd.to_datetime(line_df["period"], format="%Y-%m-%d", errors="coerce")
    line_df = line_df.dropna(subset=["period"]).set_index("period")[["revenue"]]
    if line_df.empty:
        st.warning("The timeline could not be drawn from the current slice of data.")
    else:
        st.line_chart(line_df)

# --- Top vendors ---
st.markdown("**Top vendors by revenue**")
_vendors_chart = top_vendors_df.rename(
    columns={"vendor_name": "Vendor", "revenue": "Revenue"}
)
if top_vendors_df.empty:
    st.info("Nothing to plot for this combination — try widening the date range or clearing filters.")
else:
    vendor_chart = (
        alt.Chart(_vendors_chart)
        .mark_bar()
        .encode(
            x=alt.X("Revenue:Q", title="Revenue ($)"),
            y=alt.Y("Vendor:N", sort="-x", title="Vendor"),
            tooltip=["Vendor", alt.Tooltip("Revenue:Q", format="$,.2f", title="Revenue")],
        )
        .properties(height=220)
    )
    st.altair_chart(vendor_chart, use_container_width=True)

# --- Top products ---
st.markdown("**Top menu items by revenue**")
_products_chart = top_products_df.rename(
    columns={"product_name": "Item", "revenue": "Revenue"}
)
if top_products_df.empty:
    st.info("Nothing to plot for this combination — try widening the date range or clearing filters.")
else:
    product_chart = (
        alt.Chart(_products_chart)
        .mark_bar()
        .encode(
            x=alt.X("Revenue:Q", title="Revenue ($)"),
            y=alt.Y("Item:N", sort="-x", title="Menu item"),
            tooltip=["Item", alt.Tooltip("Revenue:Q", format="$,.2f", title="Revenue")],
        )
        .properties(height=220)
    )
    st.altair_chart(product_chart, use_container_width=True)

# --- Vendor × product heatmap ---
st.markdown("**Vendor vs. menu item** — where dollars land")
_heat = vendor_product_df.rename(
    columns={
        "vendor_name": "Vendor",
        "product_name": "Item",
        "revenue": "Revenue",
    }
)
if vendor_product_df.empty:
    st.info("Nothing to plot for this combination — try widening the date range or clearing filters.")
else:
    heat = (
        alt.Chart(_heat)
        .mark_rect()
        .encode(
            x=alt.X("Item:N", title="Menu item", sort=None),
            y=alt.Y("Vendor:N", title="Vendor", sort=None),
            color=alt.Color("Revenue:Q", title="Revenue ($)"),
            tooltip=[
                "Vendor",
                "Item",
                alt.Tooltip("Revenue:Q", format="$,.2f", title="Revenue"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(heat, use_container_width=True)
