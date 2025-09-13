# worker/worker.py
import os, traceback
from dotenv import load_dotenv
load_dotenv()

# Import your app modules (assumes 'api' is a proper package)
from api.db import execute, get_conn
from api import ingest
from api.weather_backfill_adapter import backfill_weather_for_dates

__all__ = ["run_ingest_enrich", "log", "set_status", "set_ready"]

def log(job_id, msg):
    execute("update job_runs set log = coalesce(log,'') || %s where job_id=%s", (msg + "\n", job_id,))

def set_status(job_id, status):
    if status == "RUNNING":
        execute("update job_runs set status='RUNNING', started_at=now() where job_id=%s", (job_id,))
    elif status in ("SUCCESS","FAILED"):
        execute("update job_runs set status=%s, finished_at=now() where job_id=%s", (status, job_id))

def set_ready(job_id, ready: bool):
    execute("update job_runs set ready_for_prediction=%s where job_id=%s", (ready, job_id))

def run_ingest_enrich(job_id: str, file_bytes: bytes):
    """
    Enrichment job entrypoint — importable as 'worker.worker.run_ingest_enrich'
    """
    try:
        set_status(job_id, "RUNNING")
        log(job_id, "Parsing CSV …")
        df = ingest.parse_and_filter(file_bytes)
        log(job_id, f"Columns seen: {list(df.columns)[:20]}")

        log(job_id, f"Rows after filter: {len(df)}")
        upserted = ingest.upsert_daily_items_sale(df)
        log(job_id, f"Upserted rows: {upserted}")

        dates = sorted(set(df["sale_day_manual"].tolist()))
        if dates:
            log(job_id, f"Backfilling weather for {len(dates)} day(s) …")
            wcount = backfill_weather_for_dates(dates)
            log(job_id, f"Weather updated rows: {wcount}")

            log(job_id, "Setting holiday flags …")
            hcount = ingest.set_holiday_flags(dates)
            log(job_id, f"Holidays set for {hcount} day(s)")

            log(job_id, "Setting local event flags …")
            ecount = ingest.set_local_event_flags(dates)
            log(job_id, f"Local events set for {ecount} day(s)")

        set_ready(job_id, True)
        set_status(job_id, "SUCCESS")
        log(job_id, "Upload Success! Data inserted and enrichment complete.")

    except Exception as e:
        log(job_id, "ERROR: " + str(e))
        log(job_id, traceback.format_exc())
        set_status(job_id, "FAILED")
        raise
