from typing import Optional
from pydantic import BaseModel, conint, constr, model_validator

# Keep numeric bounds the same as your DB constraints
VOLTS_MIN, VOLTS_MAX = 1, 1000


class ConsumerDetailsBase(BaseModel):
    # Required business identifiers
    consumer_id: constr(strip_whitespace=True, min_length=1, max_length=50)
    circle: constr(strip_whitespace=True, min_length=1, max_length=100)

    # These were previously optional to derive from feeder/DTR;
    # feeder is removed, so we keep them optional at schema level,
    # but the router will coerce to non-empty for DB writes.
    division: Optional[constr(strip_whitespace=True, min_length=1, max_length=150)] = None
    voltage_kv: Optional[conint(gt=0, le=VOLTS_MAX)] = None

    # feeder_id REMOVED (we don't accept or store it anymore)

    sanction_load_kw: conint(gt=0)
    oa_capacity_kw: conint(ge=0)
    consumer_type: constr(strip_whitespace=True, min_length=1, max_length=50)

    # “Details” fields (optional, keep names as-is to match DB)
    Name: Optional[str] = None
    Address: Optional[str] = None
    District: Optional[str] = None
    PinCode: Optional[str] = None
    DTR_id: Optional[str] = None

    @model_validator(mode="after")
    def _ensure_voltage_default(self):
        """
        Since the UI may omit voltage_kv, choose a safe default.
        (If you later have metadata, replace this with a lookup from DTR.)
        """
        if self.voltage_kv is None:
            self.voltage_kv = 11  # default to 11 kV
        # Final sanity: clamp within bounds just in case
        if self.voltage_kv < VOLTS_MIN:
            self.voltage_kv = VOLTS_MIN
        if self.voltage_kv > VOLTS_MAX:
            self.voltage_kv = VOLTS_MAX
        return self


class ConsumerDetailsCreate(ConsumerDetailsBase):
    pass


class ConsumerDetailsUpdate(BaseModel):
    # Make all fields optional for PATCH/PUT
    circle: Optional[str] = None
    division: Optional[str] = None
    # feeder_id REMOVED — not accepted on update anymore
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
