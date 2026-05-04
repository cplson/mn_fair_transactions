"""MN Fair Sales Dashboard (Checkpoint 1: connectivity smoke test).

This stub proves the container boots, port 8501 is reachable, and the
read-only SQLite mount works. KPIs, charts, filters, and refresh polish
land in later checkpoints.
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

import streamlit as st

from dashboard.db import DB_PATH, run_query

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

st.subheader("Checkpoint 1: connectivity check")

st.write(f"Database: `{DB_PATH}`")

try:
    rows = int(run_query("SELECT COUNT(*) AS n FROM fact_sales").iloc[0]["n"])
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
    st.success(f"Connected to `fair.db`. `fact_sales` has {rows:,} row(s).")
