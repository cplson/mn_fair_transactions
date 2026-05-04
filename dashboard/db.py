"""Read-only access helpers for the dashboard.

The Streamlit app must never write to fair.db. We open the SQLite file with
URI mode `mode=ro` so any accidental write raises immediately instead of
corrupting the OLTP/OLAP store.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Tuple

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "db" / "fair.db"


def _connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"fair.db not found at {DB_PATH}. Run the ETL first, e.g. "
            "`docker compose run --rm pipeline python scripts/setup_db.py`."
        )
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)


@st.cache_data(ttl=15, show_spinner=False)
def run_query(sql: str, params: Tuple = ()) -> pd.DataFrame:
    """Run a SELECT against fair.db and return a DataFrame.

    Cached for 15s so the page reflects new Airflow loads without hammering
    SQLite on every interaction. `params` must be a tuple so cache keys stay
    hashable.
    """
    with closing(_connect()) as conn:
        return pd.read_sql_query(sql, conn, params=params)
