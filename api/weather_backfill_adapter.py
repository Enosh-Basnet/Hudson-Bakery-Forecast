# weather_backfill_adapter.py
# Refactored adapter around your existing backfill code.
# Exposes: backfill_weather_for_dates(dates: list[date]) -> int

from __future__ import annotations
import os
import time
from datetime import date, timedelta
from typing import Iterable, Dict, Any, List

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import requests

# ---------------------------
# Config (Bondi Junction / AUS)
# ---------------------------
LAT = float(os.getenv("WEATHER_LAT", "-33.8908"))
LON = float(os.getenv("WEATHER_LON", "151.2495"))
TIMEZONE = os.getenv("WEATHER_TZ", "Australia/Sydney")

# Open-Meteo hourly variables (keep aligned with original)
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation", "weathercode"]

# Chunk size for API calls (days)
CHUNK_DAYS = int(os.getenv("WEATHER_CHUNK_DAYS", "31"))

# Target table
SCHEMA = os.getenv("PGSCHEMA", "public")
TABLE = os.getenv("PGTABLE", "daily_items_sale")


# ---------------------------
# DB connection helpers
# ---------------------------
def _get_conn():
    """
    Prefer DATABASE_URL if present; otherwise fall back to SUPABASE_* parts.
    Uses sslmode=require for Supabase.
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url, sslmode="require")
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT", "5432")),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require",
    )


# ---------------------------
# Weather fetch + aggregate
# ---------------------------
def _daterange_chunks(min_date: date, max_date: date, chunk_days: int = CHUNK_DAYS):
    cur = min_date
    while cur <= max_date:
        to = min(cur + timedelta(days=chunk_days - 1), max_date)
        yield cur, to
        cur = to + timedelta(days=1)


def _fetch_hourly_weather(start_date: date, end_date: date) -> Dict[str, Any]:
    """
    Open-Meteo ERA5 archive (hourly) over a date range localized to TIMEZONE.
    """
    url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _aggregate_daily_from_hourly(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Convert hourly arrays → per-day aggregates:
      - temperature: daily mean (°C)
      - humidity   : daily mean (%)
      - precipitation: daily sum (mm)
      - weather_code: mode (most frequent) that day
    Returns: {'YYYY-MM-DD': {'temp': float|None, 'rh': float|None, 'precip': float|None, 'wmo': int|None}}
    """
    if "hourly" not in payload or not payload["hourly"].get("time"):
        return {}

    hourly = payload["hourly"]
    times = pd.to_datetime(hourly["time"])
    df = pd.DataFrame({"time": times})

    for var in HOURLY_VARS:
        df[var] = hourly.get(var, [None] * len(times))

    df["day"] = df["time"].dt.date

    agg = df.groupby("day").agg(
        temperature_2m_mean=("temperature_2m", "mean"),
        relative_humidity_2m_mean=("relative_humidity_2m", "mean"),
        precipitation_sum=("precipitation", "sum"),
    )

    mode_codes = (
        df.groupby("day")["weathercode"]
          .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else None)
          .rename("weathercode_mode")
    )

    out: Dict[str, Dict[str, Any]] = {}
    merged = agg.join(mode_codes)
    for d, row in merged.iterrows():
        out[d.isoformat()] = {
            "temp": round(float(row["temperature_2m_mean"]), 2) if pd.notnull(row["temperature_2m_mean"]) else None,
            "rh": round(float(row["relative_humidity_2m_mean"]), 2) if pd.notnull(row["relative_humidity_2m_mean"]) else None,
            "precip": round(float(row["precipitation_sum"]), 2) if pd.notnull(row["precipitation_sum"]) else None,
            "wmo": int(row["weathercode_mode"]) if pd.notnull(row["weathercode_mode"]) else None,
        }
    return out


# ---------------------------
# Public adapter API
# ---------------------------
def backfill_weather_for_dates(dates: Iterable[date]) -> int:
    """
    Backfills weather columns directly into {SCHEMA}.{TABLE} for the given sale dates.
    Returns the number of distinct dates updated.
    """
    # Normalize & guard
    distinct_dates = sorted({d for d in dates if d is not None})
    if not distinct_dates:
        return 0

    min_d, max_d = distinct_dates[0], distinct_dates[-1]

    # Fetch weather over min..max in small chunks, aggregate daily
    per_day: Dict[str, Dict[str, Any]] = {}
    for start, end in _daterange_chunks(min_d, max_d, CHUNK_DAYS):
        try:
            payload = _fetch_hourly_weather(start, end)
        except requests.HTTPError as e:
            # Skip this chunk but continue others
            continue
        daily = _aggregate_daily_from_hourly(payload)
        per_day.update(daily)
        time.sleep(0.4)  # friendly pacing

    if not per_day:
        return 0

    # Build rows only for *requested* dates we could compute
    rows: List[tuple] = []
    for d in distinct_dates:
        m = per_day.get(d.isoformat())
        if not m:
            continue
        rows.append((d, m["wmo"], m["temp"], m["rh"], m["precip"]))

    if not rows:
        return 0

    # Update table
    sql = f"""
        update {SCHEMA}.{TABLE} as t
        set weather_code = v.wmo,
            temperature = v.temp,
            humidity = v.rh,
            precipitation = v.precip
        from (values %s) as v(sale_day_manual, wmo, temp, rh, precip)
        where t.sale_day_manual = v.sale_day_manual
    """

    with _get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
        conn.commit()

    return len(rows)
