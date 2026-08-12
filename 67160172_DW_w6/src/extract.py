import json
import sqlite3
import pandas as pd
from .config import RAW_DIR, SOURCE_DB


def extract_data():
    """
    Extract data from:
      - customers.csv
      - orders.csv
      - products.json
      - stores table in store.db
    Return a dictionary of DataFrames.
    """

    # --- customers.csv ---
    # Read everything as string so we control type conversion/cleaning
    # ourselves in the transform stage instead of letting pandas guess.
    customers = pd.read_csv(RAW_DIR / "customers.csv", dtype=str)

    # --- orders.csv ---
    orders = pd.read_csv(RAW_DIR / "orders.csv", dtype=str)

    # --- products.json (nested) ---
    with open(RAW_DIR / "products.json", encoding="utf-8") as f:
        products_raw = json.load(f)
    products = pd.json_normalize(products_raw)

    # --- stores table from store.db ---
    conn = sqlite3.connect(SOURCE_DB)
    try:
        stores = pd.read_sql_query("SELECT * FROM stores", conn)
    finally:
        conn.close()

    raw = {
        "customers": customers,
        "orders": orders,
        "products": products,
        "stores": stores,
    }

    # Checkpoint: quick sanity print of shapes/columns
    for name, df in raw.items():
        print(f"[extract] {name}: shape={df.shape} columns={list(df.columns)}")

    return raw
