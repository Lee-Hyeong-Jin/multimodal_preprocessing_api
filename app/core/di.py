import os
from contextlib import asynccontextmanager
from fastapi import Depends
from app.connections.mq_publisher import MQPublisher

mq_publisher: MQPublisher | None = None

@asynccontextmanager
async def lifespan_manager(app):
    global mq_publisher
    mq_host = os.getenv("RABBITMQ_HOST", "localhost")
    mq_publisher = MQPublisher(mq_host)
    try:
        yield
    finally:
        if mq_publisher:
            mq_publisher.close()

def get_mq_publisher() -> MQPublisher:
    if not mq_publisher:
        raise RuntimeError("MQPublisher not initialized")
    return mq_publisher
