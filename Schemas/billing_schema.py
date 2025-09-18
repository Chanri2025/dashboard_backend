# Schemas/billing_schema.py
from pydantic import BaseModel, condecimal, Field
from typing import List, Optional
from datetime import datetime, date


# ----- Tariffs -----
class TariffSlabCreate(BaseModel):
    slab_from_kwh: condecimal(max_digits=14, decimal_places=4) = 0
    slab_to_kwh: Optional[condecimal(max_digits=14, decimal_places=4)] = None
    energy_rate_per_kwh: condecimal(max_digits=12, decimal_places=6)


class TariffPlanCreate(BaseModel):
    code: str
    description: Optional[str] = None
    consumer_type: str
    voltage_kv: int
    is_time_of_day: bool = False
    tax_percent: condecimal(max_digits=6, decimal_places=3) = 0
    fixed_charge: condecimal(max_digits=12, decimal_places=4) = 0
    demand_charge_per_kw: condecimal(max_digits=12, decimal_places=4) = 0
    effective_from: datetime
    effective_to: Optional[datetime] = None


class TariffPlanOut(TariffPlanCreate):
    id: int

    class Config:
        from_attributes = True


class AssignTariff(BaseModel):
    consumer_id: str
    tariff_id: int
    valid_from: datetime
    valid_to: Optional[datetime] = None


# ----- Billing -----
class BillLineOut(BaseModel):
    line_type: str
    description: Optional[str] = None
    quantity: float
    rate: float
    amount: float


class BillPreviewRequest(BaseModel):
    consumer_id: str
    meter_type: str  # 'LT' or 'OA'
    period_start: datetime
    period_end: datetime
    adjustments: Optional[condecimal(max_digits=14, decimal_places=4)] = 0


class BillStatusUpdate(BaseModel):
    status: str  # DRAFT|ISSUED|PAID|CANCELLED


class BillOut(BaseModel):
    id: Optional[int] = None
    consumer_id: str
    meter_type: str
    period_start: datetime
    period_end: datetime
    kwh_import: float
    kwh_export: float
    fixed_charge: float
    energy_charge: float
    tax_amount: float
    adjustments: float
    total_amount: float
    status: str
    lines: List[BillLineOut] = []

class ConsumerTariffBase(BaseModel):
    consumer_id: str
    tariff_id: int
    valid_from: date
    valid_to: Optional[date] = None

class ConsumerTariffCreate(ConsumerTariffBase):
    pass

class ConsumerTariffUpdate(BaseModel):
    valid_from: Optional[date] = None
    valid_to: Optional[date] = None
    tariff_id: Optional[int] = None  # Allow updating tariff assignment

class ConsumerTariffOut(ConsumerTariffBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True