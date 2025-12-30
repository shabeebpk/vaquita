from queue import Queue

job_queue = Queue()
event_queues = {}

def get_event_queue(job_id: int) -> Queue:
    if job_id not in event_queues:
        event_queues[job_id] = Queue()
    return event_queues[job_id]
