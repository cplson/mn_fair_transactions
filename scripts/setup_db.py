from pathlib import Path
import sqlite3

# ─────────────────────────────
# Project root resolution
# ─────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "db" / "fair.db"

# Ensure db folder exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────
# Database connection
# ─────────────────────────────
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ─────────────────────────────
# Tables
# ─────────────────────────────

cursor.execute("""
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id INTEGER PRIMARY KEY,
    vendor_name TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    price REAL
)
""")

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

# ─────────────────────────────
# Seed data
# ─────────────────────────────

vendors = [
    (1, "Corn Dog Stand"),
    (2, "Sweet Martha's Cookies"),
    (3, "Fresh Lemonade"),
    (4, "Cheese Curds"),
    (5, "Mini Donuts")
]

cursor.executemany(
    "INSERT OR IGNORE INTO vendors VALUES (?, ?)",
    vendors
)

products = [
    (1, "Corn Dog", 5.0),
    (2, "Cookies Bucket", 8.0),
    (3, "Lemonade", 4.0),
    (4, "Cheese Curds", 6.5),
    (5, "Mini Donuts", 7.0)
]

cursor.executemany(
    "INSERT OR IGNORE INTO products VALUES (?, ?, ?)",
    products
)

conn.commit()
conn.close()

print("Database setup complete!")