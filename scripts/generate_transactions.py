import sqlite3
import random
import time
from datetime import datetime

DB_NAME = "../data/fair.db"

def get_products(cursor):
    cursor.execute("SELECT product_id, price FROM products")
    return cursor.fetchall()

def get_vendors(cursor):
    cursor.execute("SELECT vendor_id FROM vendors")
    return [row[0] for row in cursor.fetchall()]

def generate_transaction(cursor, vendors, products):
    vendor_id = random.choice(vendors)
    product_id, price = random.choice(products)

    quantity = random.randint(1, 5)
    amount = round(quantity * price, 2)
    timestamp = datetime.now().isoformat()

    cursor.execute("""
        INSERT INTO transactions (timestamp, vendor_id, product_id, quantity, amount)
        VALUES (?, ?, ?, ?, ?)
    """, (timestamp, vendor_id, product_id, quantity, amount))

def main():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    vendors = get_vendors(cursor)
    products = get_products(cursor)

    print("Starting transaction generator... (Ctrl+C to stop)")

    try:
        while True:
            generate_transaction(cursor, vendors, products)
            conn.commit()

            print("Inserted transaction at", datetime.now().strftime("%H:%M:%S"))

            time.sleep(3)  # adjust speed here

    except KeyboardInterrupt:
        print("\nStopped generator.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()