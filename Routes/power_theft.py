# routes/power_theft.py
from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def power_theft_analysis():
    return []
