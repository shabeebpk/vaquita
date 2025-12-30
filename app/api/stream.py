from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import json

from app.core.queues import get_event_queue

router = APIRouter()

@router.get("/stream/{job_id}")
def stream(job_id: int):
    queue = get_event_queue(job_id)

    def generator():
        while True:
            event = queue.get()
            yield f"data: {json.dumps(event)}\n\n"
            if event["type"] == "done":
                break

    return StreamingResponse(generator(), media_type="text/event-stream")
