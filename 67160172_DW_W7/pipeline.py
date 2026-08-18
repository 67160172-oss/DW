"""
Python Data Pipeline Engineering - Lab Assignment
ETL Pipeline: Omnichannel Retail Sales -> Star Schema (SQLite)

Author: (student submission)
Run:    python pipeline.py
"""

from __future__ import annotations

import csv
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# --------------------------------------------------------------------------
# Task 1 - PipelineConfig
# --------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    input_path: Path
    output_db: Path
    batches: list[str]
    error_mode: Literal["quarantine", "fail_fast"] = "quarantine"
    quarantine_csv: Path = Path("output/quarantine.csv")
    run_log_csv: Path = Path("output/pipeline_run_log.csv")

    def __post_init__(self):
        self.input_path = Path(self.input_path)
        self.output_db = Path(self.output_db)
        self.quarantine_csv = Path(self.quarantine_csv)
        self.run_log_csv = Path(self.run_log_csv)


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------
APPROVED_PAYMENT_METHODS = {"cash", "credit card", "bank transfer", "promptpay"}
SALES_CHANNEL_MAP = {
    "store": "Store",
    "online": "Online",
    "marketplace": "Marketplace",
    "e-commerce": "Online",
}


def normalize_payment_method(value: str) -> str | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    if v not in APPROVED_PAYMENT_METHODS:
        return None
    # Title-case each word for display consistency
    return " ".join(w.capitalize() for w in v.split())


def normalize_sales_channel(value: str) -> str | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    return SALES_CHANNEL_MAP.get(v)


def clean_unit_price(value) -> float | None:
    """Handles values like 'THB 979.4' as well as plain numbers."""
    if pd.isna(value):
        return None
    s = str(value).replace("THB", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def clean_quantity(value) -> float | None:
    """Handles values like 'three' by treating them as unparsable (-> quarantine)."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    try:
        return float(s)
    except ValueError:
        return None


# --------------------------------------------------------------------------
# Task 1 - Extract
# --------------------------------------------------------------------------
def extract_table(path: Path, name: str) -> pd.DataFrame:
    started = datetime.now()
    try:
        df = pd.read_csv(path, dtype=str)
        elapsed = (datetime.now() - started).total_seconds()
        log.info(f"EXTRACT {name}: rows={len(df)} start={started:%H:%M:%S} elapsed={elapsed:.3f}s")
        return df
    except Exception as exc:
        log.error(f"EXTRACT {name}: FAILED ({exc})")
        raise


def extract(config: PipelineConfig, batch: str):
    customers = extract_table(config.input_path / "customers.csv", "customers")
    products = extract_table(config.input_path / "products.csv", "products")
    orders = extract_table(config.input_path / f"{batch}.csv", batch)
    return customers, products, orders


# --------------------------------------------------------------------------
# Task 2 - Transform + Data Quality
# --------------------------------------------------------------------------
def transform(orders: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame,
              already_loaded: dict[str, str], batch: str):
    """
    Returns (clean_df, quarantine_df, rows_read).
    already_loaded: {order_id: updated_at_iso} of rows already successfully loaded
    (used for incremental / idempotent processing).
    """
    rows_read = len(orders)
    df = orders.copy()
    reasons: list[list[str]] = [[] for _ in range(len(df))]

    def flag(mask, reason):
        for i in df.index[mask]:
            reasons[df.index.get_loc(i)].append(reason)

    # ---- safe type coercion -------------------------------------------------
    df["order_datetime_parsed"] = pd.to_datetime(df["order_datetime"], errors="coerce")
    flag(df["order_datetime_parsed"].isna(), "invalid_order_datetime")

    df["quantity_num"] = df["quantity"].apply(clean_quantity)
    flag(df["quantity_num"].isna(), "invalid_quantity_type")
    flag(df["quantity_num"].notna() & ((df["quantity_num"] <= 0) | (df["quantity_num"] > 20) |
                                        (df["quantity_num"] % 1 != 0)), "quantity_out_of_range")

    df["unit_price_num"] = df["unit_price"].apply(clean_unit_price)
    flag(df["unit_price_num"].isna(), "invalid_unit_price_type")
    flag(df["unit_price_num"].notna() & (df["unit_price_num"] <= 0), "unit_price_not_positive")

    df["discount_pct_num"] = pd.to_numeric(df["discount_pct"], errors="coerce")
    flag(df["discount_pct_num"].isna(), "invalid_discount_pct_type")
    flag(df["discount_pct_num"].notna() & ((df["discount_pct_num"] < 0) | (df["discount_pct_num"] > 100)),
         "discount_pct_out_of_range")

    # ---- normalize categorical fields ---------------------------------------
    df["payment_method_norm"] = df["payment_method"].apply(normalize_payment_method)
    flag(df["payment_method_norm"].isna(), "invalid_payment_method")

    df["sales_channel_norm"] = df["sales_channel"].apply(normalize_sales_channel)
    flag(df["sales_channel_norm"].isna(), "invalid_sales_channel")

    # ---- referential integrity ----------------------------------------------
    valid_customers = set(customers["customer_id"].dropna())
    valid_products = set(products["product_id"].dropna())

    flag(df["customer_id"].isna() | ~df["customer_id"].isin(valid_customers), "customer_id_not_found")
    flag(df["product_id"].isna() | ~df["product_id"].isin(valid_products), "product_id_not_found")

    # ---- updated_at parse -----------------------------------------------------
    df["updated_at_parsed"] = pd.to_datetime(df["updated_at"], errors="coerce")
    flag(df["updated_at_parsed"].isna(), "invalid_updated_at")

    df["reason_code"] = ["|".join(r) if r else "" for r in reasons]
    df["source_batch"] = batch

    # ---- split row-level valid vs invalid -------------------------------------
    row_valid_mask = df["reason_code"] == ""
    row_invalid = df[~row_valid_mask].copy()
    row_valid = df[row_valid_mask].copy()

    # ---- deduplicate by order_id, keep latest updated_at -----------------------
    row_valid = row_valid.sort_values("updated_at_parsed")
    dup_mask = row_valid.duplicated(subset="order_id", keep="last")
    duplicated_rows = row_valid[dup_mask].copy()
    duplicated_rows["reason_code"] = "duplicate_order_id_superseded"
    row_valid = row_valid[~dup_mask]

    # ---- incremental filter: skip rows already loaded with same/older updated_at
    def is_new_or_updated(r):
        prev = already_loaded.get(r["order_id"])
        if prev is None:
            return True
        return r["updated_at_parsed"] > pd.to_datetime(prev)

    incremental_mask = row_valid.apply(is_new_or_updated, axis=1)
    skipped_already_loaded = row_valid[~incremental_mask].copy()
    row_valid = row_valid[incremental_mask]

    # ---- compute derived measures ----------------------------------------------
    row_valid["gross_amount"] = row_valid["quantity_num"] * row_valid["unit_price_num"]
    row_valid["net_amount"] = row_valid["gross_amount"] * (1 - row_valid["discount_pct_num"] / 100)

    quarantine = pd.concat([row_invalid, duplicated_rows], ignore_index=True)

    return row_valid, quarantine, rows_read, skipped_already_loaded


# --------------------------------------------------------------------------
# Task 3 - Star Schema DDL + Load
# --------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id  TEXT UNIQUE NOT NULL,
    customer_name TEXT,
    province TEXT,
    segment TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id  TEXT UNIQUE NOT NULL,
    product_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT UNIQUE NOT NULL,
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT UNIQUE NOT NULL,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price > 0),
    discount_pct REAL NOT NULL CHECK (discount_pct BETWEEN 0 AND 100),
    gross_amount REAL NOT NULL CHECK (gross_amount >= 0),
    net_amount REAL NOT NULL CHECK (net_amount >= 0),
    payment_method TEXT,
    sales_channel TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_key INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    source_batch TEXT,
    reason_code TEXT,
    raw_payload TEXT,
    loaded_at TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_key INTEGER PRIMARY KEY AUTOINCREMENT,
    batch TEXT,
    started_at TEXT,
    ended_at TEXT,
    rows_read INTEGER,
    rows_valid INTEGER,
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    rows_duplicated INTEGER,
    status TEXT
);
"""


def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(DDL)
    conn.commit()


def load_dimensions(conn: sqlite3.Connection, customers: pd.DataFrame, products: pd.DataFrame):
    cur = conn.cursor()
    for _, r in customers.iterrows():
        cur.execute(
            """INSERT INTO dim_customer (customer_id, customer_name, province, segment)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(customer_id) DO UPDATE SET
                 customer_name=excluded.customer_name,
                 province=excluded.province,
                 segment=excluded.segment""",
            (r["customer_id"], r["customer_name"], r["province"], r["segment"]),
        )
    for _, r in products.iterrows():
        cur.execute(
            """INSERT INTO dim_product (product_id, product_name, category)
               VALUES (?, ?, ?)
               ON CONFLICT(product_id) DO UPDATE SET
                 product_name=excluded.product_name,
                 category=excluded.category""",
            (r["product_id"], r["product_name"], r["category"]),
        )
    conn.commit()


def get_or_create_date_key(cur: sqlite3.Cursor, ts: pd.Timestamp) -> int:
    full_date = ts.strftime("%Y-%m-%d")
    date_key = int(ts.strftime("%Y%m%d"))
    cur.execute(
        """INSERT INTO dim_date (date_key, full_date, day, month, quarter, year)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(date_key) DO NOTHING""",
        (date_key, full_date, ts.day, ts.month, (ts.month - 1) // 3 + 1, ts.year),
    )
    return date_key


def load_facts(conn: sqlite3.Connection, clean: pd.DataFrame) -> tuple[int, int]:
    """Upsert clean rows into fact_sales inside one transaction. Returns (loaded, ignored_dupe)."""
    cur = conn.cursor()
    loaded = 0
    for _, r in clean.iterrows():
        date_key = get_or_create_date_key(cur, r["order_datetime_parsed"])
        cur.execute("SELECT customer_key FROM dim_customer WHERE customer_id=?", (r["customer_id"],))
        cust_row = cur.fetchone()
        cur.execute("SELECT product_key FROM dim_product WHERE product_id=?", (r["product_id"],))
        prod_row = cur.fetchone()
        if not cust_row or not prod_row:
            continue  # safety net; should already be filtered in transform()
        cur.execute(
            """INSERT INTO fact_sales
               (order_id, date_key, customer_key, product_key, quantity, unit_price,
                discount_pct, gross_amount, net_amount, payment_method, sales_channel, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(order_id) DO UPDATE SET
                 date_key=excluded.date_key,
                 customer_key=excluded.customer_key,
                 product_key=excluded.product_key,
                 quantity=excluded.quantity,
                 unit_price=excluded.unit_price,
                 discount_pct=excluded.discount_pct,
                 gross_amount=excluded.gross_amount,
                 net_amount=excluded.net_amount,
                 payment_method=excluded.payment_method,
                 sales_channel=excluded.sales_channel,
                 updated_at=excluded.updated_at
               WHERE excluded.updated_at > fact_sales.updated_at""",
            (
                r["order_id"], date_key, cust_row[0], prod_row[0],
                int(r["quantity_num"]), float(r["unit_price_num"]), float(r["discount_pct_num"]),
                float(r["gross_amount"]), float(r["net_amount"]),
                r["payment_method_norm"], r["sales_channel_norm"],
                r["updated_at_parsed"].isoformat(),
            ),
        )
        loaded += 1
    conn.commit()
    return loaded, 0


def load_quarantine(conn: sqlite3.Connection, quarantine: pd.DataFrame):
    if quarantine.empty:
        return
    cur = conn.cursor()
    now = datetime.now().isoformat()
    for _, r in quarantine.iterrows():
        payload_cols = ["order_id", "order_datetime", "customer_id", "product_id", "quantity",
                         "unit_price", "discount_pct", "payment_method", "sales_channel", "updated_at"]
        payload = "; ".join(f"{c}={r.get(c)}" for c in payload_cols)
        cur.execute(
            """INSERT INTO quarantine (order_id, source_batch, reason_code, raw_payload, loaded_at)
               VALUES (?, ?, ?, ?, ?)""",
            (r.get("order_id"), r.get("source_batch"), r.get("reason_code"), payload, now),
        )
    conn.commit()


def write_run_log(conn: sqlite3.Connection, batch, started, ended, rows_read, rows_valid,
                   rows_loaded, rows_rejected, rows_duplicated, status):
    conn.execute(
        """INSERT INTO pipeline_run_log
           (batch, started_at, ended_at, rows_read, rows_valid, rows_loaded,
            rows_rejected, rows_duplicated, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (batch, started.isoformat(), ended.isoformat(), rows_read, rows_valid,
         rows_loaded, rows_rejected, rows_duplicated, status),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Task 4/5 - Orchestration
# --------------------------------------------------------------------------
def get_watermark(conn: sqlite3.Connection) -> dict[str, str]:
    """order_id -> updated_at (iso) for all order_ids ever loaded (via fact_sales)."""
    try:
        cur = conn.execute("SELECT order_id, updated_at FROM fact_sales")
        return {r[0]: r[1] for r in cur.fetchall()}
    except sqlite3.OperationalError:
        return {}


def run_pipeline(config: PipelineConfig, batch: str) -> dict:
    started = datetime.now()
    log.info(f"=== RUN START batch={batch} ===")
    conn = sqlite3.connect(config.output_db)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_schema(conn)

    status = "success"
    rows_read = rows_valid = rows_loaded = rows_rejected = rows_dup = 0
    try:
        customers, products, orders = extract(config, batch)
        load_dimensions(conn, customers, products)

        watermark = get_watermark(conn)
        clean, quarantine, rows_read, skipped = transform(orders, customers, products, watermark, batch)

        rows_valid = len(clean) + len(skipped)
        rows_rejected = len(quarantine)
        rows_dup = int((quarantine["reason_code"] == "duplicate_order_id_superseded").sum()) if not quarantine.empty else 0

        loaded, _ = load_facts(conn, clean)
        rows_loaded = loaded
        load_quarantine(conn, quarantine)

        log.info(f"TRANSFORM {batch}: read={rows_read} valid={rows_valid} "
                 f"rejected={rows_rejected} skipped_already_loaded={len(skipped)}")
        log.info(f"LOAD {batch}: loaded={rows_loaded} quarantined={rows_rejected}")
    except Exception as exc:
        status = "failed"
        log.error(f"RUN {batch}: FAILED - {exc} (previously loaded data left intact)")
        conn.rollback()
    finally:
        ended = datetime.now()
        write_run_log(conn, batch, started, ended, rows_read, rows_valid, rows_loaded,
                      rows_rejected, rows_dup, status)
        conn.close()

    log.info(f"=== RUN END batch={batch} status={status} "
             f"read={rows_read} loaded={rows_loaded} rejected={rows_rejected} ===\n")
    return dict(batch=batch, status=status, rows_read=rows_read, rows_valid=rows_valid,
                rows_loaded=rows_loaded, rows_rejected=rows_rejected, rows_duplicated=rows_dup)


# --------------------------------------------------------------------------
# Export helpers (quarantine.csv, pipeline_run_log.csv, KPI summary)
# --------------------------------------------------------------------------
def export_quarantine_csv(config: PipelineConfig):
    conn = sqlite3.connect(config.output_db)
    df = pd.read_sql_query("SELECT * FROM quarantine", conn)
    conn.close()
    config.quarantine_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.quarantine_csv, index=False)
    return df


def export_run_log_csv(config: PipelineConfig):
    conn = sqlite3.connect(config.output_db)
    df = pd.read_sql_query("SELECT * FROM pipeline_run_log", conn)
    conn.close()
    config.run_log_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.run_log_csv, index=False)
    return df


def kpi_summary(config: PipelineConfig):
    conn = sqlite3.connect(config.output_db)
    fact_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    net_sum = conn.execute("SELECT COALESCE(SUM(net_amount),0) FROM fact_sales").fetchone()[0]
    quarantine_count = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
    conn.close()
    return dict(fact_rows=fact_count, total_net_sales=round(net_sum, 2), quarantined_rows=quarantine_count)


# --------------------------------------------------------------------------
# Main - demonstrates idempotency + incremental load across 4 runs
# --------------------------------------------------------------------------
if __name__ == "__main__":
    config = PipelineConfig(
        input_path=Path("data"),
        output_db=Path("output/retail_dw.db"),
        batches=["orders_batch_1", "orders_batch_2", "orders_batch_3"],
    )
    config.output_db.parent.mkdir(parents=True, exist_ok=True)

    # remove any previous db for a clean demonstration
    if config.output_db.exists():
        config.output_db.unlink()

    results = []
    results.append(run_pipeline(config, "orders_batch_1"))          # run 1
    results.append(run_pipeline(config, "orders_batch_1"))          # run 2 - idempotency check
    results.append(run_pipeline(config, "orders_batch_2"))          # run 3
    results.append(run_pipeline(config, "orders_batch_3"))          # run 4

    export_quarantine_csv(config)
    export_run_log_csv(config)

    print("\n=== RUN RESULTS ===")
    for r in results:
        print(r)

    print("\n=== KPI SUMMARY ===")
    print(kpi_summary(config))
