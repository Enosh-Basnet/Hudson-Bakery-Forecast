import os, uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from .db import execute, fetchone, get_conn
from . import ingest
from .weather_backfill_adapter import backfill_weather_for_dates
from .models import JobStatus
import os, redis
from rq import Queue
from dotenv import load_dotenv
load_dotenv()
app = FastAPI(title="Hudson's API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
r = redis.Redis.from_url(os.environ["REDIS_URL"], socket_keepalive=True)
q = Queue("pipeline", connection=r)

@app.post("/ingest_enrich")
async def ingest_enrich(file: UploadFile = File(...), started_by: str = "admin@hudsons"):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Please upload a CSV file")

    job_id = str(uuid.uuid4())
    execute("insert into job_runs(job_id, started_by, status) values (%s,%s,'QUEUED')", (job_id, started_by))

    file_bytes = await file.read()
    # enqueue job with the raw bytes
    q.enqueue("worker.worker.run_ingest_enrich", job_id, file_bytes, job_timeout=60*20)
    return {"job_id": job_id}

@app.get("/jobs/{job_id}", response_model=JobStatus)
def job_status(job_id: str):
    row = fetchone("select status, ready_for_prediction, started_at, finished_at, log from job_runs where job_id=%s", (job_id,))
    if not row:
        raise HTTPException(404, "Unknown job")
    return JobStatus(
        status=row["status"],
        ready_for_prediction=row["ready_for_prediction"],
        started_at=row["started_at"].isoformat() if row["started_at"] else None,
        finished_at=row["finished_at"].isoformat() if row["finished_at"] else None,
        log=row.get("log")
    )
 # py -m uvicorn api.main:app --reload --port 8000