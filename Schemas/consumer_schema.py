from typing import Optional
from pydantic import BaseModel, conint, constr


class ConsumerDetailsBase(BaseModel):
    consumer_id: constr(strip_whitespace=True, min_length=1, max_length=50)
    circle: constr(strip_whitespace=True, min_length=1, max_length=100)
    division: constr(strip_whitespace=True, min_length=1, max_length=150)
    voltage_kv: conint(gt=0, le=1000)
    sanction_load_kw: conint(gt=0)
    oa_capacity_kw: conint(ge=0)
    consumer_type: constr(strip_whitespace=True, min_length=1, max_length=50)

    Name: Optional[str] = None
    Address: Optional[str] = None
    District: Optional[str] = None
    PinCode: Optional[str] = None
    DTR_id: Optional[str] = None


class ConsumerDetailsCreate(ConsumerDetailsBase):
    pass


class ConsumerDetailsUpdate(BaseModel):
    circle: Optional[str] = None
    division: Optional[str] = None
    voltage_kv: Optional[int] = None
    sanction_load_kw: Optional[int] = None
    oa_capacity_kw: Optional[int] = None
    consumer_type: Optional[str] = None
    Name: Optional[str] = None
    Address: Optional[str] = None
    District: Optional[str] = None
    PinCode: Optional[str] = None
    DTR_id: Optional[str] = None


class ConsumerDetailsOut(ConsumerDetailsBase):
    id: int

    class Config:
        from_attributes = True
