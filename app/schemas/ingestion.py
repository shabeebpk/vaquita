from pydantic import BaseModel
from typing import List

class IngestionResponse(BaseModel):
    job_id: int
    uploaded_files: List[str]
