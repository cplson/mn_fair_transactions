import sqlite3
import csv
from pathlib import Path

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
        print("No new transactions.")
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

    last_id = rows[-1][0]
    update_last_extracted_id(last_id)

    print(f"Extracted {len(rows)} new transactions. Last ID: {last_id}")

    conn.close()

# ─────────────────────────────
# Entry point
# ─────────────────────────────
if __name__ == "__main__":
    extract_new_transactions()