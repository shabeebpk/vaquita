# events.py
import redis
import json

r = redis.Redis(host="localhost", port=6379, db=2)

def publish_event(payload: dict, channel: str = None, user_id: int = 1):
    target_channel = channel or f"user:{user_id}"
    r.publish(target_channel, json.dumps(payload))
