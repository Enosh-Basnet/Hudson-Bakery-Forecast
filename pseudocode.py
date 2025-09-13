def run_ingest_enrich(job_id, file_bytes):
    mark(job_id, "RUNNING")
    try:
        # 1) Parse CSV → strict whitelist
        df = parse_and_filter(file_bytes)  # returns columns: sale_day_manual, item_name, variation_id, quantity, price
        df = normalize_and_validate(df)    # types, trim, dedupe

        # 2) UPSERT into daily_items_sale
        upsert_daily_items_sale(df)

        # Distinct dates affected
        dates = sorted(df["sale_day_manual"].dropna().unique().tolist())

        # 3) Weather backfill (you provide this module)
        count_weather = backfill_weather(conn, dates)  # updates weather_* in the same table

        # 4) Holiday flag (0/1)
        count_holidays = set_holiday_flags(conn, dates)  # updates is_holiday

        # 5) Local event flag (0/1)
        count_events = set_local_event_flags(conn, dates, area="Bondi Junction")  # updates is_local_event

        # 6) Finish & gate prediction
        append_log(job_id, f"Upload Success! Rows upserted: {len(df)} | weather:{count_weather} | holidays:{count_holidays} | events:{count_events}")
        set_ready_for_prediction(job_id, True)
        mark(job_id, "SUCCESS")
    except Exception as e:
        append_log(job_id, f"ERROR: {e}")
        mark(job_id, "FAILED")
        raise
