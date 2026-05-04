import csv
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# ─────────────────────────────
# Project paths (single source of truth)
# ─────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "db" / "fair.db"

STATE_FILE = DATA_DIR / "state" / "last_extracted_id.txt"
OUTPUT_FILE = DATA_DIR / "staging" / "staging_transactions.csv"

# Ensure required directories exist
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────
# State helpers
# ─────────────────────────────
def get_last_extracted_id():
    if not STATE_FILE.exists():
        return 0
    return int(STATE_FILE.read_text().strip() or 0)

def update_last_extracted_id(last_id):
    STATE_FILE.write_text(str(last_id))

# ─────────────────────────────
# Extraction logic
# ─────────────────────────────
def extract_new_transactions():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    last_id = get_last_extracted_id()

    cursor.execute("""
        SELECT transaction_id, timestamp, vendor_id, product_id, quantity, amount
        FROM transactions
        WHERE transaction_id > ?
        ORDER BY transaction_id ASC
    """, (last_id,))

    rows = cursor.fetchall()

    if not rows:
        logger.info(
            "Extracted 0 new transactions (watermark transaction_id=%s, no rows above it).",
            last_id,
        )
        conn.close()
        return

    file_exists = OUTPUT_FILE.exists()

    with OUTPUT_FILE.open("a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "transaction_id",
                "timestamp",
                "vendor_id",
                "product_id",
                "quantity",
                "amount"
            ])

        writer.writerows(rows)

    new_high = rows[-1][0]
    update_last_extracted_id(new_high)

    logger.info(
        "Extracted %s new transaction(s); watermark advanced from transaction_id=%s to %s; appended to %s",
        len(rows),
        last_id,
        new_high,
        OUTPUT_FILE,
    )

    conn.close()

# ─────────────────────────────
# Entry point
# ─────────────────────────────
if __name__ == "__main__":
    extract_new_transactions()