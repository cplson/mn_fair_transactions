"""
MN Fair ELT: incremental extract to staging, then OLAP load in SQLite.

Scripts assume project root at /opt/airflow/mn_fair (see docker-compose.airflow.yml).
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SCRIPTS_DIR = Path("/opt/airflow/mn_fair/scripts")


def _ensure_scripts_on_path() -> None:
    p = str(SCRIPTS_DIR)
    if p not in sys.path:
        sys.path.insert(0, p)


def run_extract() -> None:
    _ensure_scripts_on_path()
    from extract_transactions import extract_new_transactions

    extract_new_transactions()


def run_load_olap() -> None:
    _ensure_scripts_on_path()
    from load_to_olap import main

    main()


default_args = {
    "owner": "mn_fair",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}

with DAG(
    dag_id="mn_fair_elt",
    default_args=default_args,
    description="Extract new transactions to staging CSV, then load OLAP star schema.",
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mn_fair", "elt"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_transactions",
        python_callable=run_extract,
    )
    load_task = PythonOperator(
        task_id="load_to_olap",
        python_callable=run_load_olap,
    )
    extract_task >> load_task
