import json
import asyncio
import redis.asyncio as redis
from fastapi import APIRouter, FastAPI
from fastapi.responses import StreamingResponse

router = APIRouter()


from app.config.system_settings import system_settings
# r = redis.Redis(host=os.getenv("REDIS_URL"), port=6379, db=2)
r = redis.from_url(system_settings.REDIS_URL)

async def event_stream(user_id: int = 1):
    pubsub = r.pubsub()
    await pubsub.subscribe(f"user:{user_id}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                yield f"data: {message['data'].decode()}\n\n"
    finally:
        await pubsub.unsubscribe(f"user:{user_id}")
        await pubsub.close()

@router.get("/user/{user_id}/events/")
async def sse(user_id: int = 1):
    return StreamingResponse(
        event_stream(user_id=user_id),
        media_type="text/event-stream"
    )
