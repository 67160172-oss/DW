import numpy as np
import pandas as pd
from .config import PROVINCE_MAP


# Explicit list of date formats seen in orders.csv (mixed formats).
# Tried in order; the first one that parses cleanly wins.
DATE_FORMATS = [
    "%Y-%m-%d",   # 2026-08-01
    "%Y/%m/%d",   # 2026/08/02
    "%d/%m/%Y",   # 01/08/2026
    "%d-%b-%Y",   # 03-Aug-2026
]


def _parse_mixed_date(value):
    """Try each known format; return pd.Timestamp or NaT if none match."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(text, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT


def _standardize_province(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    # try exact match first (handles Thai script, which has no case)
    if text in PROVINCE_MAP:
        return PROVINCE_MAP[text]
    # fall back to case-insensitive match for English variants
    lowered = text.lower()
    if lowered in PROVINCE_MAP:
        return PROVINCE_MAP[lowered]
    return text  # unknown value, keep as-is rather than losing information


def _clean_customers(customers_raw):
    df = customers_raw.copy()

    # remove duplicate customer_id (keep first occurrence)
    before = len(df)
    df = df.drop_duplicates(subset="customer_id", keep="first")
    dupes_removed = before - len(df)

    # standardize province
    df["province"] = df["province"].apply(_standardize_province)

    # handle missing email
    df["email"] = df["email"].fillna("").astype(str).str.strip()
    df["email"] = df["email"].replace("", "unknown@example.com")

    df = df.reset_index(drop=True)
    print(f"[transform] customers: removed {dupes_removed} duplicate customer_id rows")
    return df


def _clean_products(products_raw):
    df = products_raw.copy()

    # flatten + rename nested fields produced by json_normalize
    rename_map = {}
    if "category.name" in df.columns:
        rename_map["category.name"] = "category"
    if "pricing.price" in df.columns:
        rename_map["pricing.price"] = "price"
    df = df.rename(columns=rename_map)

    # convert price to numeric (handles values like "1,299.00")
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    # missing category -> "Unknown"
    df["category"] = df["category"].fillna("Unknown")
    df.loc[df["category"].astype(str).str.strip() == "", "category"] = "Unknown"
    df.loc[df["category"].astype(str) == "None", "category"] = "Unknown"

    keep_cols = ["product_id", "product_name", "category", "price"]
    df = df[keep_cols].reset_index(drop=True)
    return df


def _clean_orders(orders_raw):
    """
    Clean orders, validate business rules, and split into valid rows
    and rejected rows (with a reject_reason).
    """
    df = orders_raw.copy()

    # remove duplicate order_id (keep first occurrence)
    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    dupes_removed = before - len(df)
    print(f"[transform] orders: removed {dupes_removed} duplicate order_id rows")

    # normalize status
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    # numeric coercion (invalid values become NaN so they can be flagged)
    df["qty_num"] = pd.to_numeric(df["qty"], errors="coerce")
    df["unit_price_num"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct_num"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    # parse mixed date formats
    df["order_date_parsed"] = df["order_date"].apply(_parse_mixed_date)

    # build per-row reject reasons (a row can fail more than one rule)
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)

    bad_qty = df["qty_num"].isna() | (df["qty_num"] <= 0)
    reasons[bad_qty] = reasons[bad_qty].apply(lambda r: r + ["qty<=0 or invalid"])

    bad_price = df["unit_price_num"].isna() | (df["unit_price_num"] <= 0)
    reasons[bad_price] = reasons[bad_price].apply(lambda r: r + ["unit_price<=0 or invalid"])

    bad_discount = (
        df["discount_pct_num"].isna()
        | (df["discount_pct_num"] < 0)
        | (df["discount_pct_num"] > 100)
    )
    reasons[bad_discount] = reasons[bad_discount].apply(lambda r: r + ["discount_pct out of range"])

    bad_date = df["order_date_parsed"].isna()
    reasons[bad_date] = reasons[bad_date].apply(lambda r: r + ["invalid order_date"])

    df["reject_reason"] = reasons.apply(lambda r: "; ".join(r) if r else "")
    is_invalid = df["reject_reason"] != ""

    rejects = df[is_invalid].copy()
    rejects["reject_stage"] = "orders_validation"

    valid = df[~is_invalid].copy()
    valid["qty"] = valid["qty_num"].astype(int)
    valid["unit_price"] = valid["unit_price_num"].astype(float)
    valid["discount_pct"] = valid["discount_pct_num"].astype(float)
    valid["order_date"] = valid["order_date_parsed"]

    drop_cols = ["qty_num", "unit_price_num", "discount_pct_num", "order_date_parsed", "reject_reason"]
    valid = valid.drop(columns=drop_cols)

    return valid.reset_index(drop=True), rejects.reset_index(drop=True)


def transform_data(raw):
    """
    Clean customers/products/orders, merge into sales fact rows, and
    return (clean_customers, clean_products, sales, rejects).
    """
    clean_customers = _clean_customers(raw["customers"])
    clean_products = _clean_products(raw["products"])
    valid_orders, order_rejects = _clean_orders(raw["orders"])

    # keep only paid / completed orders for the sales fact
    status_ok = valid_orders["status"].isin(["paid", "completed"])
    sales_candidates = valid_orders[status_ok].copy()
    other_status_rejects = valid_orders[~status_ok].copy()
    other_status_rejects["reject_reason"] = "status not paid/completed (" + other_status_rejects["status"] + ")"
    other_status_rejects["reject_stage"] = "status_filter"

    # join customers + products; reject unknown customer/product
    known_customers = set(clean_customers["customer_id"])
    known_products = set(clean_products["product_id"])

    unknown_cust = ~sales_candidates["customer_id"].isin(known_customers)
    unknown_prod = ~sales_candidates["product_id"].isin(known_products)
    unknown_mask = unknown_cust | unknown_prod

    unknown_rejects = sales_candidates[unknown_mask].copy()

    def _ref_reason(row_cust_unknown, row_prod_unknown):
        parts = []
        if row_cust_unknown:
            parts.append("unknown customer_id")
        if row_prod_unknown:
            parts.append("unknown product_id")
        return "; ".join(parts)

    unknown_rejects["reject_reason"] = [
        _ref_reason(c, p)
        for c, p in zip(unknown_cust[unknown_mask], unknown_prod[unknown_mask])
    ]
    unknown_rejects["reject_stage"] = "referential_integrity"

    merged = sales_candidates[~unknown_mask].merge(
        clean_products[["product_id", "product_name", "category", "price"]],
        on="product_id",
        how="left",
        suffixes=("", "_product"),
    )

    # calculate amounts
    merged["gross_amount"] = merged["qty"] * merged["unit_price"]
    merged["discount_amount"] = merged["gross_amount"] * merged["discount_pct"] / 100
    merged["sales_amount"] = merged["gross_amount"] - merged["discount_amount"]

    sales_cols = [
        "order_id", "customer_id", "product_id", "order_date",
        "qty", "unit_price", "discount_pct",
        "gross_amount", "discount_amount", "sales_amount", "status",
    ]
    sales = merged[sales_cols].reset_index(drop=True)

    # combine all rejects into a single frame with a consistent schema
    reject_frames = [order_rejects, other_status_rejects, unknown_rejects]
    common_cols = ["order_id", "customer_id", "product_id", "order_date",
                    "qty", "unit_price", "discount_pct", "status",
                    "reject_reason", "reject_stage"]
    normalized = []
    for f in reject_frames:
        f = f.copy()
        for c in common_cols:
            if c not in f.columns:
                f[c] = None
        normalized.append(f[common_cols])
    rejects = pd.concat(normalized, ignore_index=True) if normalized else pd.DataFrame(columns=common_cols)

    print(f"[transform] sales rows: {len(sales)} | rejected rows: {len(rejects)}")

    return clean_customers, clean_products, sales, rejects
