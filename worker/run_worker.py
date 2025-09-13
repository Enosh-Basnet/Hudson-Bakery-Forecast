# worker/run_worker.py
import os
import sys
import platform
from pathlib import Path
from dotenv import load_dotenv
import redis
from rq import Worker, Queue

# SimpleWorker avoids os.fork(); needed on Windows
try:
    from rq import SimpleWorker
except Exception:
    SimpleWorker = None  # older RQ may not have it

# --- Make sure the project root is on sys.path ---
# project_root/
#   api/
#   worker/
#   ui/
#   ...
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]  # adjust if your layout differs
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

def get_conn():
    # Requires REDIS_URL in env
    return redis.Redis.from_url(os.environ["REDIS_URL"], socket_keepalive=True)

if __name__ == "__main__":
    conn = get_conn()
    q = Queue("pipeline", connection=conn)

    is_windows = (os.name == "nt") or (platform.system().lower() == "windows")
    if is_windows and SimpleWorker is not None:
        print("[worker] Windows detected → using RQ SimpleWorker (no fork).")
        w = SimpleWorker([q], connection=conn)
        w.work(with_scheduler=False)
    else:
        print("[worker] Using standard RQ Worker.")
        w = Worker([q], connection=conn)
        w.work(with_scheduler=False)



# py worker/run_worker.py