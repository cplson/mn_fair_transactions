"""Central SQL definitions and parameterized builders for the dashboard."""

from __future__ import annotations

# --- Filter option sources (no sidebar params) ---

DATE_BOUNDS_DIM_TIME = """
SELECT
    MIN(date(timestamp)) AS dmin,
    MAX(date(timestamp)) AS dmax
FROM dim_time;
"""

DIM_VENDOR_OPTIONS = """
SELECT vendor_id, vendor_name
FROM dim_vendor
ORDER BY vendor_name;
"""

DIM_PRODUCT_OPTIONS = """
SELECT product_id, product_name
FROM dim_product
ORDER BY product_name;
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


def _vendor_product_clause(
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    """Extra WHERE fragments + bound params (empty tuples mean no filter / all)."""
    parts: list[str] = []
    extra: list[int] = []
    if vendor_ids:
        ph = ",".join("?" * len(vendor_ids))
        parts.append(f"f.vendor_id IN ({ph})")
        extra.extend(vendor_ids)
    if product_ids:
        ph = ",".join("?" * len(product_ids))
        parts.append(f"f.product_id IN ({ph})")
        extra.extend(product_ids)
    frag = (" AND " + " AND ".join(parts)) if parts else ""
    return frag, tuple(extra)


def kpi_total_revenue(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT COALESCE(SUM(f.amount), 0.0) AS total_revenue
FROM fact_sales AS f
JOIN dim_time AS t ON f.time_id = t.time_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
"""
    return sql.strip(), (date_start, date_end) + extra


def kpi_total_transactions(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT COUNT(*) AS total_transactions
FROM fact_sales AS f
JOIN dim_time AS t ON f.time_id = t.time_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
"""
    return sql.strip(), (date_start, date_end) + extra


def kpi_average_ticket(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT COALESCE(AVG(f.amount), 0.0) AS average_ticket
FROM fact_sales AS f
JOIN dim_time AS t ON f.time_id = t.time_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
"""
    return sql.strip(), (date_start, date_end) + extra


def kpi_active_vendors(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT COUNT(DISTINCT f.vendor_id) AS active_vendors
FROM fact_sales AS f
JOIN dim_time AS t ON f.time_id = t.time_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
"""
    return sql.strip(), (date_start, date_end) + extra


def star_schema_sample(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
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
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
ORDER BY f.sale_id DESC
LIMIT 20
"""
    return sql.strip(), (date_start, date_end) + extra


def chart_revenue_over_time(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT
    printf('%04d-%02d-%02d', t.year, t.month, t.day) AS period,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_time AS t
    ON f.time_id = t.time_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
GROUP BY t.year, t.month, t.day
ORDER BY t.year, t.month, t.day
"""
    return sql.strip(), (date_start, date_end) + extra


def chart_top_vendors(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT
    v.vendor_name,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_time AS t
    ON f.time_id = t.time_id
JOIN dim_vendor AS v
    ON f.vendor_id = v.vendor_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
GROUP BY v.vendor_id, v.vendor_name
ORDER BY revenue DESC
"""
    return sql.strip(), (date_start, date_end) + extra


def chart_top_products(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT
    p.product_name,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_time AS t
    ON f.time_id = t.time_id
JOIN dim_product AS p
    ON f.product_id = p.product_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC
"""
    return sql.strip(), (date_start, date_end) + extra


def chart_vendor_product_revenue(
    date_start: str,
    date_end: str,
    vendor_ids: tuple[int, ...],
    product_ids: tuple[int, ...],
) -> tuple[str, tuple]:
    frag, extra = _vendor_product_clause(vendor_ids, product_ids)
    sql = f"""
SELECT
    v.vendor_name,
    p.product_name,
    SUM(f.amount) AS revenue
FROM fact_sales AS f
JOIN dim_time AS t
    ON f.time_id = t.time_id
JOIN dim_vendor AS v
    ON f.vendor_id = v.vendor_id
JOIN dim_product AS p
    ON f.product_id = p.product_id
WHERE date(t.timestamp) BETWEEN date(?) AND date(?){frag}
GROUP BY v.vendor_id, v.vendor_name, p.product_id, p.product_name
"""
    return sql.strip(), (date_start, date_end) + extra
