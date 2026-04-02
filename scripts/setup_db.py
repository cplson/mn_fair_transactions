import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_NAME = os.path.join(BASE_DIR, "data", "fair.db")

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# Vendors
cursor.execute("""
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id INTEGER PRIMARY KEY,
    vendor_name TEXT
)
""")

# Products
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    price REAL
)
""")

# Transactions
cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    vendor_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    amount REAL,
    FOREIGN KEY(vendor_id) REFERENCES vendors(vendor_id),
    FOREIGN KEY(product_id) REFERENCES products(product_id)
)
""")

# Seed vendors
vendors = [
    (1, "Corn Dog Stand"),
    (2, "Sweet Martha's Cookies"),
    (3, "Fresh Lemonade"),
    (4, "Cheese Curds"),
    (5, "Mini Donuts")
]

cursor.executemany("INSERT OR IGNORE INTO vendors VALUES (?, ?)", vendors)

# Seed products
products = [
    (1, "Corn Dog", 5.0),
    (2, "Cookies Bucket", 8.0),
    (3, "Lemonade", 4.0),
    (4, "Cheese Curds", 6.5),
    (5, "Mini Donuts", 7.0)
]

cursor.executemany("INSERT OR IGNORE INTO products VALUES (?, ?, ?)", products)

conn.commit()
conn.close()

print("Database setup complete!")