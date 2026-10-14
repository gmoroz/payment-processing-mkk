"""HTTP API."""

from fastapi import APIRouter

from app.api.payments.routes import router as payments_router

api_router = APIRouter()
api_router.include_router(payments_router)
