# events.py
import redis
import json
import os

# r = redis.Redis(host=os.getenv("REDIS_URL"), port=6379, db=2)
r = redis.from_url(os.getenv("REDIS_URL"))

def publish_event(payload: dict, channel: str = None, user_id: int = 1):
    target_channel = channel or f"user:{user_id}"
    r.publish(target_channel, json.dumps(payload))
