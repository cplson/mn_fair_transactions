# Ubuntu-based runtime for pipeline scripts (sqlite / CSV). Keeps Python and OS
# consistent across machines. Airflow is optional: see docker-compose.airflow.yml.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3.12 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.12 /usr/local/bin/python3 \
    && ln -sf /usr/local/bin/python3 /usr/local/bin/python

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY scripts ./scripts
COPY dashboard ./dashboard

# Persist database and staging CSV via: -v "$(pwd)/data:/app/data"
VOLUME ["/app/data"]

# Examples (from repo root, with compose):
#   docker compose run --rm pipeline python scripts/setup_db.py
#   docker compose run --rm pipeline python scripts/extract_transactions.py
#   docker compose up dashboard            # Streamlit UI on http://localhost:8501
CMD ["sh", "-c", "printf '%s\n' \
  'mn_fair_pipeline image' \
  '  docker compose run --rm pipeline python scripts/setup_db.py' \
  '  docker compose run --rm pipeline python scripts/extract_transactions.py' \
  '  docker compose up dashboard            # Streamlit UI on http://localhost:8501' \
  'Optional Airflow: docker compose -f docker-compose.airflow.yml up' \
  && exit 0"]
