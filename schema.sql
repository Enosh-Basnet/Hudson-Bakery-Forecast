-- create a staging table and importing the unpivoted file data into it:
CREATE TABLE IF NOT EXISTS public.staging_sales_unpivot (
  item_name           TEXT,
  item_variation_name TEXT,
  category_name       TEXT,
  sale_day_manual     DATE NOT NULL,
  quantity            INTEGER NOT NULL CHECK (quantity >= 0),

  -- Monday=0 … Sunday=6 (Postgres EXTRACT(DOW) is Sun=0, so rotate)
  sale_dow SMALLINT
    GENERATED ALWAYS AS ( ((EXTRACT(DOW FROM sale_day_manual)::INT + 6) % 7) ) STORED
);


-- adding weather columns
ALTER TABLE public.daily_items_sale
  ADD COLUMN IF NOT EXISTS weather_code   INTEGER,         -- WMO code (e.g., 61 = rain)
  ADD COLUMN IF NOT EXISTS temperature    NUMERIC(5,2),    -- °C (daily mean from hourly)
  ADD COLUMN IF NOT EXISTS humidity       NUMERIC(5,2),    -- % (daily mean from hourly)
  ADD COLUMN IF NOT EXISTS precipitation  NUMERIC(6,2);    -- mm (daily sum from hourly)

-- track the one-click run (for UI status)
create table if not exists job_runs (
  job_id uuid primary key default gen_random_uuid(),
  kind text not null default 'INGEST_ENRICH',
  started_by text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'QUEUED',        -- QUEUED|RUNNING|SUCCESS|FAILED
  log text default '',
  ready_for_prediction boolean default false
);
