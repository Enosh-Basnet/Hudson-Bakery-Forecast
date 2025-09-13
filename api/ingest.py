# api/ingest.py
import io, unicodedata
import pandas as pd
from .db import get_conn
from psycopg2.extras import execute_values

# ---------- Header normalization & mapping ----------

def _clean_header(name: str) -> str:
    if name is None:
        return ""
    s = unicodedata.normalize("NFKC", str(name))
    s = s.replace("\ufeff", "").strip()  # strip BOM + trim
    s = " ".join(s.split())               # collapse inner spaces
    return s.lower().replace(" ", "_")

# Map many possible CSV headers to our DB columns
HEADER_ALIASES = {
    # sale_day_manual
    "sale_day_manual": "sale_day_manual",
    "sale_date":       "sale_day_manual",
    "order_date":      "sale_day_manual",
    "date":            "sale_day_manual",

    # item_name
    "item_name": "item_name",
    "name":      "item_name",
    "item":      "item_name",

    # item_variation_name
    "item_variation_name": "item_variation_name",
    "item_variation":      "item_variation_name",
    "variation_name":      "item_variation_name",
    "variation":           "item_variation_name",

    # category_name
    "category_name": "category_name",
    "category":      "category_name",
    "cat_name":      "category_name",

    # variation_id
    "variation_id":      "variation_id",
    "item_variation_id": "variation_id",
    "sku":               "variation_id",

    # quantity
    "quantity": "quantity",
    "qty":      "quantity",
}

REQUIRED = ["sale_day_manual", "item_name", "quantity"]

# ---------- Parse & prepare DataFrame ----------

def parse_and_filter(file_bytes: bytes) -> pd.DataFrame:
    # Let pandas sniff delimiter; keep raw strings initially
    df = pd.read_csv(io.BytesIO(file_bytes), sep=None, engine="python", dtype=str)
    df.columns = [_clean_header(c) for c in df.columns]

    # Build normalized frame via alias map
    out = {}
    for raw in df.columns:
        target = HEADER_ALIASES.get(raw)
        if target:
            out[target] = df[raw]
    f = pd.DataFrame(out)

    # Derive sale_day_manual from created_at if needed
    if "sale_day_manual" not in f.columns and "created_at" in df.columns:
        ts = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
        try:
            ts = ts.dt.tz_convert("Australia/Sydney")
        except Exception:
            pass
        f["sale_day_manual"] = sdm.dt.date   # keep as Python date objects


    # Validate required columns
    missing = [c for c in REQUIRED if c not in f.columns]
    if missing:
        sample = ", ".join(list(df.columns)[:25])
        raise ValueError(
            f"Missing required column(s) after mapping: {missing}. "
            f"Available headers (first 25): {sample}"
        )

    # Coerce / clean
    # Coerce / clean
    sdm = pd.to_datetime(f["sale_day_manual"], errors="coerce", dayfirst=True)
    if sdm.isna().any():
        bad = f.loc[sdm.isna(), "sale_day_manual"].head(5).tolist()
        raise ValueError(f"sale_day_manual has unparsable dates (examples: {bad})")
    f["sale_day_manual"] = sdm.dt.date   # <-- keep as date objects


    f["item_name"] = f["item_name"].astype(str).str.strip()

    if "item_variation_name" in f.columns:
        f["item_variation_name"] = f["item_variation_name"].astype(str).str.strip()
    else:
        f["item_variation_name"] = None

    if "category_name" in f.columns:
        f["category_name"] = f["category_name"].astype(str).str.strip()
    else:
        f["category_name"] = None

    f["quantity"] = pd.to_numeric(f["quantity"], errors="coerce").fillna(0).astype(int)

    if "variation_id" in f.columns:
        f["variation_id"] = f["variation_id"].fillna("NA").astype(str).replace({"": "NA"})
    else:
        f["variation_id"] = "NA"

    # Drop empties & de-dup by natural key (date+item+variation)
    f = f.dropna(subset=["sale_day_manual", "item_name"])
    f = f.drop_duplicates(subset=["sale_day_manual", "item_name", "variation_id"], keep="last")

    # Return exactly the columns we insert
    return f[[
        "sale_day_manual",
        "item_name",
        "item_variation_name",
        "category_name",
        "variation_id",
        "quantity",
    ]]

# ---------- Upsert ----------

def upsert_daily_items_sale(df: pd.DataFrame) -> int:
    rows = [(
        r.sale_day_manual,
        r.item_name,
        r.item_variation_name,
        r.category_name,
        r.variation_id,
        int(r.quantity),
    ) for r in df.itertuples(index=False)]

    sql = """
    INSERT INTO daily_items_sale
        (sale_day_manual, item_name, item_variation_name, category_name, variation_id, quantity)
    VALUES %s
    ON CONFLICT (sale_day_manual, item_name, variation_id)
    DO UPDATE SET
        item_variation_name = EXCLUDED.item_variation_name,
        category_name       = EXCLUDED.category_name,
        quantity            = EXCLUDED.quantity
    """
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        conn.commit()
    return len(rows)

# ---------- Flags (unchanged logic placeholders) ----------

def set_holiday_flags(dates: list) -> int:
    import holidays
    years = sorted({d.year for d in dates})
    au = holidays.Australia(years=years)
    items = [(d, 1 if d in au else 0) for d in dates]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE t_h (d date PRIMARY KEY, h smallint) ON COMMIT DROP;")
        execute_values(cur, "INSERT INTO t_h(d,h) VALUES %s", items)
        cur.execute("""
            UPDATE daily_items_sale d
            SET is_holiday = t.h
            FROM t_h t
            WHERE d.sale_day_manual = t.d
        """)
        conn.commit()
    return len(items)

def set_local_event_flags(dates: list, default_zero=True) -> int:
    # Replace with real event logic later
    items = [(d, 0) for d in dates]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE t_e (d date PRIMARY KEY, e smallint) ON COMMIT DROP;")
        execute_values(cur, "INSERT INTO t_e(d,e) VALUES %s", items)
        cur.execute("""
            UPDATE daily_items_sale d
            SET is_local_event = t.e
            FROM t_e t
            WHERE d.sale_day_manual = t.d
        """)
        conn.commit()
    return len(items)
