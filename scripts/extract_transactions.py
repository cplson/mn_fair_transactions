import sqlite3
import csv
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_NAME = os.path.join(BASE_DIR, "data", "fair.db")
TRACK_FILE = "data/last_extracted_id.txt"
OUTPUT_FILE = "data/staging_transactions.csv"

def get_last_extracted_id():
    if not os.path.exists(TRACK_FILE):
        return 0
    with open(TRACK_FILE, "r") as f:
        return int(f.read().strip())

def update_last_extracted_id(last_id):
    with open(TRACK_FILE, "w") as f:
        f.write(str(last_id))

def extract_new_transactions():
    conn = sqlite3.connect(DB_NAME)
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

    # Write to CSV (append mode)
    file_exists = os.path.isfile(OUTPUT_FILE)

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        # Write header only once
        if not file_exists:
            writer.writerow(["transaction_id", "timestamp", "vendor_id", "product_id", "quantity", "amount"])

        writer.writerows(rows)

    # Update last processed ID
    last_id = rows[-1][0]
    update_last_extracted_id(last_id)

    print(f"Extracted {len(rows)} new transactions. Last ID: {last_id}")

    conn.close()

if __name__ == "__main__":
    extract_new_transactions()