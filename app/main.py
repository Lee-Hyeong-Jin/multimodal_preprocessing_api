from fastapi import FastAPI
from app.core.di import lifespan_manager
from app.api.v1.routers import api_v1_router

app = FastAPI(
    title="preprocessing api",
    description="API Documents",
    version="1.0.0",
    lifespan=lifespan_manager  
)

app.include_router(api_v1_router, prefix="/api/v1")

