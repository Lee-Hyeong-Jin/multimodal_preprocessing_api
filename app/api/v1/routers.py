from fastapi import APIRouter
from app.api.v1.endpoints import pdf

api_v1_router = APIRouter()
api_v1_router.include_router(pdf.router, prefix="/pdf", tags=["pdf"])
