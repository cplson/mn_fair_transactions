"""Central SQL definitions for the Streamlit dashboard."""

KPI_TOTAL_REVENUE = """
SELECT COALESCE(SUM(amount), 0.0) AS total_revenue
FROM fact_sales;
"""

KPI_TOTAL_TRANSACTIONS = """
SELECT COUNT(*) AS total_transactions
FROM fact_sales;
"""

KPI_AVERAGE_TICKET = """
SELECT COALESCE(AVG(amount), 0.0) AS average_ticket
FROM fact_sales;
"""

KPI_ACTIVE_VENDORS = """
SELECT COUNT(DISTINCT vendor_id) AS active_vendors
FROM fact_sales;
"""

ROW_COUNTS = """
SELECT 'dim_vendor' AS table_name, COUNT(*) AS row_count FROM dim_vendor
UNION ALL
SELECT 'dim_product' AS table_name, COUNT(*) AS row_count FROM dim_product
UNION ALL
SELECT 'dim_time' AS table_name, COUNT(*) AS row_count FROM dim_time
UNION ALL
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales;
"""

STAR_SCHEMA_SAMPLE = """
SELECT
    f.transaction_id,
    t.timestamp,
    v.vendor_name,
    p.product_name,
    f.quantity,
    f.amount
FROM fact_sales AS f
JOIN dim_time AS t
    ON f.time_id = t.time_id
JOIN dim_vendor AS v
    ON f.vendor_id = v.vendor_id
JOIN dim_product AS p
    ON f.product_id = p.product_id
ORDER BY f.sale_id DESC
LIMIT 20;
"""

# --- Charts (Checkpoint 3): full fact table, no sidebar filters yet ---

CHART_REVENUE_OVER_TIME = """
SELECT
    printf(
        '%04d-%02d-%02d %02d:00',
        t.year,
        t.month,
        t.day,
        t.hour
    ) AS period,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_time AS t
    ON f.time_id = t.time_id
GROUP BY t.year, t.month, t.day, t.hour
ORDER BY t.year, t.month, t.day, t.hour;
"""

CHART_TOP_VENDORS = """
SELECT
    v.vendor_name,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_vendor AS v
    ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_id, v.vendor_name
ORDER BY revenue DESC;
"""

CHART_TOP_PRODUCTS = """
SELECT
    p.product_name,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_product AS p
    ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC;
"""

CHART_VENDOR_PRODUCT_REVENUE = """
SELECT
    v.vendor_name,
    p.product_name,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_vendor AS v
    ON f.vendor_id = v.vendor_id
JOIN dim_product AS p
    ON f.product_id = p.product_id
GROUP BY v.vendor_id, v.vendor_name, p.product_id, p.product_name;
"""
