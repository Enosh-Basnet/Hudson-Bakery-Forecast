# 🍞 Hudson’s Bakery – Real-Time Forecasting Pipeline

This project ingests bakery sales data (from CSV), enriches it with weather + holiday + local event features, and stores everything in a PostgreSQL database. A Streamlit UI lets non-technical users upload data and monitor progress, while background workers (RQ + Redis) handle ingestion and enrichment jobs.

---

## 📋 Prerequisites

Make sure you have these installed:

- [Python 3.10+](https://www.python.org/downloads/) (we tested on 3.13)
- [PostgreSQL](https://www.postgresql.org/) (Supabase is used in production)
- [Redis](https://redis.io/) (we use [Upstash](https://upstash.com/) in production)
- [Git](https://git-scm.com/) (to clone the repo)
- (Optional) [Poetry](https://python-poetry.org/) or `venv` for virtual environments

---

## 🛠️ Project structure

```
hudson/
│
├── api/
│   ├── db.py                  # DB connection helpers
│   ├── ingest.py              # CSV parsing + upsert logic
│   ├── weather_backfill_adapter.py # Weather enrichment
│   └── ... other helpers
│
├── worker/
│   ├── worker.py              # Job functions (run_ingest_enrich, flags, etc.)
│   └── run_worker.py          # Worker runner (SimpleWorker on Windows)
│
├── ui/
│   └── app.py                 # Streamlit dashboard
│
├── requirements.txt           # Python dependencies
├── .env.example               # Example environment variables
└── README.md                  # This file
```

---

## ⚙️ Step 1. Clone & create environment

```bash
git clone https://github.com/yourname/hudsons-bakery.git
cd hudsons-bakery

```

---

## 📦 Step 2. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

(Dependencies include `psycopg2-binary`, `redis`, `rq`, `streamlit`, `pandas`, `python-dotenv`, `holidays`.)

---

## 🔑 Step 3. Configure environment variables

Copy `.env.example` to `.env` and fill in your own values:

```ini
# PostgreSQL (Supabase)
SUPABASE_HOST=db.xxxxx.supabase.co
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=your_password

# Redis (Upstash or local Redis URL)
REDIS_URL=redis://default:password@host:port

# API base (FastAPI service if you’re running one)
API_BASE=http://localhost:8000
```

⚠️ **Never commit your real `.env` file** — keep it secret.

---

## 🗄️ Step 4. Set up your database

Run these SQL commands once to make sure your `daily_items_sale` table has the right schema and unique constraint:

```sql
-- Basic table
CREATE TABLE IF NOT EXISTS daily_items_sale (
    sale_day_manual date NOT NULL,
    item_name text NOT NULL,
    item_variation_name text,
    category_name text,
    variation_id text,
    quantity integer DEFAULT 0,
    sale_dow smallint GENERATED ALWAYS AS (EXTRACT(DOW FROM sale_day_manual)) STORED,
    weather_code int,
    temperature numeric,
    humidity numeric,
    precipitation numeric,
    is_holiday smallint DEFAULT 0,
    is_local_event smallint DEFAULT 0
);

-- Uniqueness by (date + item + variation)
ALTER TABLE daily_items_sale
ADD CONSTRAINT daily_items_sale_unique
UNIQUE (sale_day_manual, item_name, variation_id);

-- Jobs table (tracks background jobs)
CREATE TABLE IF NOT EXISTS job_runs (
    job_id uuid PRIMARY KEY,
    status text DEFAULT 'PENDING',
    log text,
    started_at timestamp,
    finished_at timestamp,
    ready_for_prediction boolean DEFAULT false
);
```
## ⚡ Step 5. Run the FastAPI backend (Uvicorn)

The API (in `api/main.py`) exposes endpoints for file uploads, job status checks, and integrations with the worker.

Start it with [Uvicorn](https://www.uvicorn.org/):

```bash
# From project root
uvicorn api.main:app --reload --port 8000
```

- `api.main:app` → points to the FastAPI instance in `api/main.py`.
- `--reload` → auto-reloads the server when you change code.
- `--port 8000` → binds the server to port 8000.

Now the API is available at [http://localhost:8000](http://localhost:8000).

⚠️ You should run this **before** starting the worker and UI, because they rely on the API endpoints.

---
---

## 🚀 Step 5. Run the background worker

The worker processes ingestion jobs from the Redis queue.

### Windows
```bash
python worker/run_worker.py
```
(Uses `SimpleWorker` because Windows doesn’t support `os.fork`.)

### macOS/Linux
```bash
python worker/run_worker.py
```
(Uses the normal forking `Worker`.)

You should see logs like:
```
[worker] Windows detected → using RQ SimpleWorker (no fork).
19:12:45 Worker started, waiting for jobs...
```

---

## 💻 Step 6. Launch the UI

In a separate terminal:

```bash
streamlit run ui/app.py --server.port 8501
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

What you’ll see:
- Upload a CSV file (`sales_unpivot.csv` format).
- Click **Upload & Enrich**.
- The worker processes your file: parses → inserts → enriches with weather/holiday/events.
- Logs update in real time.
- If successful, you’ll see a **Start Prediction** button (future ML hook).

---

## 📊 Sample CSV format

Your CSV should have headers like:

```csv
sale_day_manual,item_name,item_variation_name,category_name,variation_id,quantity
2025-09-10,Croissant,Butter Croissant,Pastries,var123,12
2025-09-10,Baguette,Classic Baguette,Breads,var456,7
```

Aliases are supported (`order_date`, `variation_name`, `qty`, etc.), but we recommend using the canonical names above.

---

## 🔧 Troubleshooting

- **KeyError: 'REDIS_URL'**  
  → Ensure `.env` is loaded and has `REDIS_URL`.  

- **TypeError: can only concatenate str (not "timedelta")**  
  → Make sure `sale_day_manual` is stored as a date, not string (we fixed this).  

- **psycopg2.errors.InvalidColumnReference (ON CONFLICT)**  
  → Ensure you added the unique constraint on `(sale_day_manual, item_name, variation_id)`.  

- **StreamlitDuplicateElementKey**  
  → Always give unique `key=` values to `st.text_area` or re-use placeholders.

---

## 🧑‍🤝‍🧑 Contributing

- Keep commits small & focused.
- Add clear logs for worker jobs (`worker/worker.py` → `log()`).
- Ensure any new DB columns are reflected in `ingest.py`.

---

## 📜 License

MIT — free to use & modify. Please credit Hudson’s Bakery Capstone Project Team.
