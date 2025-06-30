from fastapi import FastAPI
from app.api.v1.routers import api_v1_router

app = FastAPI(
    title="preprocessing api",
    description="API Documents",
    version="1.0.0",
)

app.include_router(api_v1_router, prefix="/api/v1")

