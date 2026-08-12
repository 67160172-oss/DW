import sqlite3
from .config import WAREHOUSE_DB


def _get_conn():
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(WAREHOUSE_DB)


def _create_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            province TEXT,
            email TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            price REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            product_id TEXT,
            order_date TEXT,
            qty INTEGER,
            unit_price REAL,
            discount_pct REAL,
            sales_amount REAL
        )
    """)
    conn.commit()


def load_data(customers, products, sales):
    """
    Load dim_customer, dim_product, fact_sales into the SQLite warehouse.

    Uses INSERT OR REPLACE keyed on each table's primary key so the
    pipeline is idempotent: running it twice with the same source data
    updates existing rows in place instead of duplicating them.
    """
    conn = _get_conn()
    try:
        _create_tables(conn)
        cur = conn.cursor()

        cur.executemany(
            "INSERT OR REPLACE INTO dim_customer (customer_id, name, province, email) "
            "VALUES (?, ?, ?, ?)",
            customers[["customer_id", "name", "province", "email"]].itertuples(index=False, name=None),
        )

        cur.executemany(
            "INSERT OR REPLACE INTO dim_product (product_id, product_name, category, price) "
            "VALUES (?, ?, ?, ?)",
            products[["product_id", "product_name", "category", "price"]].itertuples(index=False, name=None),
        )

        sales_to_load = sales.copy()
        sales_to_load["order_date"] = sales_to_load["order_date"].astype(str)
        cur.executemany(
            "INSERT OR REPLACE INTO fact_sales "
            "(order_id, customer_id, product_id, order_date, qty, unit_price, discount_pct, sales_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            sales_to_load[[
                "order_id", "customer_id", "product_id", "order_date",
                "qty", "unit_price", "discount_pct", "sales_amount",
            ]].itertuples(index=False, name=None),
        )

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM dim_customer")
        n_cust = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dim_product")
        n_prod = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_sales")
        n_sales = cur.fetchone()[0]
        print(f"[load] dim_customer={n_cust} dim_product={n_prod} fact_sales={n_sales}")
    finally:
        conn.close()
