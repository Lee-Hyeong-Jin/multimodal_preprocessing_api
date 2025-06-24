import os
from contextlib import asynccontextmanager
from fastapi import Depends
from app.connections.mq_publisher import MQPublisher
from app.core.config import settings

mq_publisher: MQPublisher | None = None

@asynccontextmanager
async def lifespan_manager(app):
    global mq_publisher
    mq_host = settings.RABBITMQ_HOST
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
