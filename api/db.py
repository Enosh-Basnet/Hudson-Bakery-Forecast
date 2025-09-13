import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def _conn_params_from_env():
    """
    Prefer explicit params (SUPABASE_*). Fall back to DATABASE_URL if present.
    """
    host = os.getenv("SUPABASE_HOST")
   

    # Explicit param path
    host = host or ""
    port = int(os.getenv("SUPABASE_PORT", "5432"))
    dbname = os.getenv("SUPABASE_DB", "")
    user = os.getenv("SUPABASE_USER", "")
    password = os.getenv("SUPABASE_PASSWORD", "")

    missing = [k for k, v in {
        "SUPABASE_HOST": host,
        "SUPABASE_DB": dbname,
        "SUPABASE_USER": user,
        "SUPABASE_PASSWORD": password,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required env vars: {', '.join(missing)}. "
            "Either set SUPABASE_* values or provide DATABASE_URL."
        )

    return {
        "use_dsn": False,
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
        "sslmode": "require",  # Supabase requires SSL
    }

def get_conn():
    params = _conn_params_from_env()
    if params.get("use_dsn"):
        return psycopg2.connect(params["dsn"], cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
        sslmode=params["sslmode"],
        cursor_factory=RealDictCursor,
    )

def fetchone(sql, params=()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def execute(sql, params=()):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()

def execute_values_insert(cur, sql_prefix, rows):
    from psycopg2.extras import execute_values
    execute_values(cur, sql_prefix + " VALUES %s", rows)
