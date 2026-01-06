"""
Task queues for asynchronous processing.

Queues:
- job_queue: Main job processing queue (status transitions, orchestration)
- extraction_queue: Text extraction tasks (read files, extract text, create IngestionSource)
- event_queues: Per-job event queues for SSE streaming
"""

from queue import Queue

# Main job processing queue
job_queue = Queue()

# File extraction/text extraction queue
extraction_queue = Queue()

# Per-job event queues for streaming updates
event_queues = {}


def get_event_queue(job_id: int) -> Queue:
    """Get or create the event queue for a job."""
    if job_id not in event_queues:
        event_queues[job_id] = Queue()
    return event_queues[job_id]
