from pydantic import BaseModel

class JobStatus(BaseModel):
    status: str
    ready_for_prediction: bool
    started_at: str | None = None
    finished_at: str | None = None
    log: str | None = None
