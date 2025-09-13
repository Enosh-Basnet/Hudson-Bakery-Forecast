#!/usr/bin/env python3
"""
Backfill daily weather into daily_items_sale using Open-Meteo.

- Location: Bondi Junction, Sydney (approx -33.8908, 151.2495)
- Timezone: Australia/Sydney (prevents off-by-one-day issues)
- Aggregation:
    * temperature: daily mean of hourly temperature_2m (°C)
    * humidity   : daily mean of hourly relative_humidity_2m (%)
    * precipitation: daily sum of hourly precipitation (mm)
    * weather_code: most frequent hourly weathercode that day

Requires: psycopg2, requests, pandas
  pip install psycopg2-binary requests pandas
"""

import psycopg2
from dotenv import load_dotenv
load_dotenv()
import os
import math
import time
import collections
from datetime import date, timedelta
import requests

import pandas as pd

# ---------- CONFIG ----------
PG_HOST     = os.getenv("SUPABASE_HOST")
PG_PORT     = 5432
PG_DBNAME   = os.getenv("SUPABASE_DB")
PG_USER     = os.getenv("SUPABASE_USER")
PG_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SCHEMA      = os.getenv("PGSCHEMA",   "public")
TABLE       = os.getenv("PGTABLE",    "daily_items_sale")


# Bondi Junction (Sydney)
LAT = -33.8908
LON = 151.2495
TIMEZONE = "Australia/Sydney"

# Open-Meteo hourly variables
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation", "weathercode"]

# Chunk window (days) per API call; 31 is a safe, readable chunk size
CHUNK_DAYS = 31

# ---------- DB HELPERS ----------
def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require"   # <-- add this for Supabase
    )

def fetch_distinct_dates(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT sale_day_manual
            FROM {SCHEMA}.{TABLE}
            WHERE sale_day_manual IS NOT NULL
            ORDER BY sale_day_manual;
        """)
        return [r[0] for r in cur.fetchall()]

def update_weather_rows(conn, rows):
    """
    rows: iterable of (day, weather_code, temperature, humidity, precipitation)
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(f"""
                UPDATE {SCHEMA}.{TABLE}
                SET weather_code = %s,
                    temperature = %s,
                    humidity = %s,
                    precipitation = %s
                WHERE sale_day_manual = %s;
            """, (r[1], r[2], r[3], r[4], r[0]))
    conn.commit()


# ---------- WEATHER FETCH ----------
def daterange_chunks(dates, chunk_days=31):
    """Yield (start_date, end_date) inclusive for contiguous ranges that cover the set of dates."""
    if not dates:
        return
    # If dates are sparse, we still just cover min..max (fewer API calls). That’s fine for historical.
    start = min(dates)
    end   = max(dates)
    cur = start
    while cur <= end:
        to = min(cur + timedelta(days=chunk_days - 1), end)
        yield (cur, to)
        cur = to + timedelta(days=1)

def fetch_hourly_weather(start_date: date, end_date: date):
    """
    Use the Open-Meteo archive API (best for historical ranges) to fetch hourly weather.
    Docs: https://archive-api.open-meteo.com/
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

def aggregate_daily_from_hourly(payload):
    """
    Convert Open-Meteo hourly arrays to per-day aggregates.
    Returns dict: { 'YYYY-MM-DD': {'temp': x, 'rh': y, 'precip': z, 'wmo': k}, ... }
    """
    if "hourly" not in payload or not payload["hourly"].get("time"):
        return {}

    hourly = payload["hourly"]
    times = pd.to_datetime(hourly["time"])  # already localized to timezone param
    df = pd.DataFrame({"time": times})

    for var in HOURLY_VARS:
        df[var] = hourly.get(var, [None] * len(times))

    df["day"] = df["time"].dt.date

    # Daily means & sums
    agg = df.groupby("day").agg(
        temperature_2m_mean=("temperature_2m", "mean"),
        relative_humidity_2m_mean=("relative_humidity_2m", "mean"),
        precipitation_sum=("precipitation", "sum"),
    )

    # Weather code: most frequent (mode) per day
    mode_codes = (
        df.groupby("day")["weathercode"]
          .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else None)
          .rename("weathercode_mode")
    )

    out = {}
    merged = agg.join(mode_codes)
    for day, row in merged.iterrows():
        out[day.isoformat()] = {
            "temp": round(float(row["temperature_2m_mean"]), 2) if pd.notnull(row["temperature_2m_mean"]) else None,
            "rh": round(float(row["relative_humidity_2m_mean"]), 2) if pd.notnull(row["relative_humidity_2m_mean"]) else None,
            "precip": round(float(row["precipitation_sum"]), 2) if pd.notnull(row["precipitation_sum"]) else None,
            "wmo": int(row["weathercode_mode"]) if pd.notnull(row["weathercode_mode"]) else None,
        }
    return out

# ---------- MAIN ----------
def main():
    conn = get_conn()
    try:
        # 1) Get dates to backfill
        dates = fetch_distinct_dates(conn)
        if not dates:
            print("No sale_day_manual dates found in table; nothing to do.")
            return

        print(f"Found {len(dates)} distinct sale dates: {min(dates)} → {max(dates)}")

        # 2) Fetch weather in chunks and collect rows
        per_day = {}  # 'YYYY-MM-DD' -> metrics dict
        for (start, end) in daterange_chunks(dates, CHUNK_DAYS):
            print(f"Fetching weather: {start} .. {end}")
            try:
                payload = fetch_hourly_weather(start, end)
            except requests.HTTPError as e:
                print(f"[WARN] HTTP error for {start}..{end}: {e}")
                continue
            except requests.RequestException as e:
                print(f"[WARN] Request error for {start}..{end}: {e}")
                continue

            daily = aggregate_daily_from_hourly(payload)
            per_day.update(daily)

            # Friendly pacing to be nice to the API
            time.sleep(0.5)

        if not per_day:
            print("No weather data fetched; aborting update.")
            return

        # 3) Build temp rows for only the dates we actually have in the table
        rows = []
        for d in dates:
            key = d.isoformat()
            m = per_day.get(key)
            if not m:
                continue
            rows.append((
                d,
                m["wmo"],
                m["temp"],
                m["rh"],
                m["precip"],
            ))

        if not rows:
            print("No matching date rows to update.")
            return

        print(f"Updating {len(rows)} date(s) directly in {SCHEMA}.{TABLE}...")
        update_weather_rows(conn, rows)


        print("Weather columns backfilled successfully!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
