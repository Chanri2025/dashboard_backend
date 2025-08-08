# main.py
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

from routes.routes_auth import router as auth_router
from routes.adjustment import router as adjustment_router
from routes.availability import router as availability_router
from routes.backdown import router as backdown_router
from routes.banking import router as banking_router
from routes.consolidated import router as consolidated_router
from routes.consumer import router as consumer_router
from routes.demand import router as demand_router
from routes.dtr import router as dtr_router
from routes.feeder import router as feeder_router
from routes.iex import router as iex_router
from routes.plant import router as plant_router
from routes.power_theft import router as power_theft_router
from routes.procurement import router as procurement_router
from routes.region import router as region_router
from routes.substation import router as substation_router

load_dotenv()

app = FastAPI(title="Power Casting API", debug=True)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mongo connection
mongo_uri = os.getenv("MONGO_URI")
mongo_client = AsyncIOMotorClient(mongo_uri)
app.state.mongo_db = mongo_client["powercasting"]

# Register Routers
app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(adjustment_router, prefix="/adjusting", tags=["Adjustment"])
app.include_router(availability_router, prefix="/availability", tags=["Availability"])
app.include_router(backdown_router, prefix="/backdown", tags=["Backdown"])
app.include_router(banking_router, prefix="/banking", tags=["Banking"])
app.include_router(
    consolidated_router, prefix="/consolidated-part", tags=["Consolidated"]
)
app.include_router(consumer_router, prefix="/consumer", tags=["Consumer"])
app.include_router(demand_router, prefix="/demand", tags=["Demand"])
app.include_router(dtr_router, prefix="/dtr", tags=["DTR"])
app.include_router(feeder_router, prefix="/feeder", tags=["Feeder"])
app.include_router(iex_router, prefix="/iex", tags=["IEX"])
app.include_router(plant_router, prefix="/plant", tags=["Plant"])
app.include_router(power_theft_router, prefix="/power-theft", tags=["Power - Theft"])
app.include_router(procurement_router, prefix="/procurement", tags=["Procurement"])
app.include_router(region_router, prefix="/region", tags=["Region"])
app.include_router(substation_router, prefix="/substation", tags=["Sub - Station"])


@app.get("/")
async def root():
    return {"message": "GUVNL is running!"}
