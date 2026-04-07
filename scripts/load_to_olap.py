import sqlite3
import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_NAME = os.path.join(BASE_DIR, "data", "fair.db")

def create_tables(cursor):
    cursor.executescript("""
    -- DIMENSIONS
    CREATE TABLE IF NOT EXISTS dim_vendor (
        vendor_id INTEGER PRIMARY KEY,
        vendor_name TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_product (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        price REAL
    );

    CREATE TABLE IF NOT EXISTS dim_time (
        time_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        hour INTEGER,
        day INTEGER,
        month INTEGER,
        year INTEGER
    );

    -- FACT TABLE
    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id INTEGER,
        vendor_id INTEGER,
        product_id INTEGER,
        time_id INTEGER,
        quantity INTEGER,
        amount REAL,
        FOREIGN KEY (vendor_id) REFERENCES dim_vendor(vendor_id),
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
        FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
    );
    """)

def load_dimensions(cursor):
    print("Loading dimension tables...")

    cursor.execute("""
        INSERT OR IGNORE INTO dim_vendor (vendor_id, vendor_name)
        SELECT vendor_id, vendor_name FROM vendors;
    """)

    cursor.execute("""
        INSERT OR IGNORE INTO dim_product (product_id, product_name, price)
        SELECT product_id, product_name, price FROM products;
    """)

    cursor.execute("""
        INSERT INTO dim_time (timestamp, hour, day, month, year)
        SELECT DISTINCT
            t.timestamp,
            CAST(strftime('%H', t.timestamp) AS INTEGER),
            CAST(strftime('%d', t.timestamp) AS INTEGER),
            CAST(strftime('%m', t.timestamp) AS INTEGER),
            CAST(strftime('%Y', t.timestamp) AS INTEGER)
        FROM transactions t
        LEFT JOIN dim_time dt
            ON t.timestamp = dt.timestamp
        WHERE dt.timestamp IS NULL;
    """)

def load_fact_table(cursor):
    print("Loading fact table...")

    cursor.execute("""
        INSERT INTO fact_sales (
            transaction_id,
            vendor_id,
            product_id,
            time_id,
            quantity,
            amount
        )
        SELECT
            t.transaction_id,
            t.vendor_id,
            t.product_id,
            dt.time_id,
            t.quantity,
            t.amount
        FROM transactions t
        JOIN dim_time dt
            ON t.timestamp = dt.timestamp
        LEFT JOIN fact_sales fs
            ON t.transaction_id = fs.transaction_id
        WHERE fs.transaction_id IS NULL;
    """)

def main():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print("Starting OLAP load...")

    create_tables(cursor)
    load_dimensions(cursor)
    load_fact_table(cursor)

    conn.commit()
    conn.close()

    print("Load complete!")

if __name__ == "__main__":
    main()