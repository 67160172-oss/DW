import sqlite3
from .config import WAREHOUSE_DB


def validate_data(source_sales):
    """
    Compare the transformed (source) sales data against what actually
    landed in the warehouse and return a validation summary dict.
    """
    conn = sqlite3.connect(WAREHOUSE_DB)
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM fact_sales")
        warehouse_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM fact_sales")
        duplicate_order_ids = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(sales_amount), 0) FROM fact_sales")
        warehouse_total_sales = round(cur.fetchone()[0], 2)
    finally:
        conn.close()

    source_valid_rows = len(source_sales)
    source_total_sales = round(source_sales["sales_amount"].sum(), 2)

    status = "PASS" if (
        source_valid_rows == warehouse_rows
        and duplicate_order_ids == 0
        and abs(source_total_sales - warehouse_total_sales) < 0.01
    ) else "FAIL"

    result = {
        "source_valid_rows": source_valid_rows,
        "warehouse_rows": warehouse_rows,
        "duplicate_order_ids": duplicate_order_ids,
        "source_total_sales": source_total_sales,
        "warehouse_total_sales": warehouse_total_sales,
        "status": status,
    }
    return result
