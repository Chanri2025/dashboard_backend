# Schemas/complaints_schema.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class ComplaintCategoryCreate(BaseModel):
    code: str
    description: Optional[str] = None


class ComplaintCategoryOut(ComplaintCategoryCreate):
    id: int

    class Config:
        from_attributes = True


class ComplaintCreate(BaseModel):
    consumer_id: str
    category_code: str
    title: str
    description: str
    priority: str = "MEDIUM"


class ComplaintOut(BaseModel):
    id: int
    consumer_id: str
    category_code: str
    title: str
    description: str
    status: str
    priority: str
    assigned_to: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime] = None


class ComplaintNoteCreate(BaseModel):
    author: str
    note: str


class ComplaintNoteOut(BaseModel):
    id: int
    author: str
    note: str
    created_at: datetime


class ComplaintWithNotesOut(ComplaintOut):
    notes: List[ComplaintNoteOut] = []
