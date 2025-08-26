# routers/complaints.py
from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from Connections.db_sql_consumers import get_db
from Models.complaints_models import Base as ComplaintsBase
from Models.complaints_models import ComplaintCategory, Complaint, ComplaintNote
from Schemas.complaints_schema import (
    ComplaintCategoryCreate, ComplaintCategoryOut,
    ComplaintCreate, ComplaintOut, ComplaintNoteCreate, ComplaintNoteOut, ComplaintWithNotesOut
)

router = APIRouter()


# ---------- Categories ----------
@router.post("/categories", response_model=ComplaintCategoryOut, status_code=status.HTTP_201_CREATED)
def create_category(body: ComplaintCategoryCreate, db: Session = Depends(get_db)):
    exists = db.execute(select(ComplaintCategory).where(ComplaintCategory.code == body.code)).scalar_one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="Category code already exists")
    obj = ComplaintCategory(**body.model_dump())
    db.add(obj);
    db.commit();
    db.refresh(obj)
    return obj


@router.get("/categories", response_model=List[ComplaintCategoryOut])
def list_categories(db: Session = Depends(get_db)):
    rows = db.execute(select(ComplaintCategory)).scalars().all()
    return rows


# ---------- Complaints ----------
@router.post("/", response_model=ComplaintOut, status_code=status.HTTP_201_CREATED)
def create_complaint(body: ComplaintCreate, db: Session = Depends(get_db)):
    cat = db.execute(select(ComplaintCategory).where(ComplaintCategory.code == body.category_code)).scalar_one_or_none()
    if not cat:
        raise HTTPException(status_code=404, detail="Category not found")
    c = Complaint(
        consumer_id=body.consumer_id,
        category_id=cat.id,
        title=body.title,
        description=body.description,
        priority=body.priority
    )
    db.add(c);
    db.commit();
    db.refresh(c)
    return ComplaintOut(
        id=c.id, consumer_id=c.consumer_id, category_code=body.category_code,
        title=c.title, description=c.description, status=c.status, priority=c.priority,
        assigned_to=c.assigned_to, created_at=c.created_at, updated_at=c.updated_at, resolved_at=c.resolved_at
    )


@router.get("/", response_model=List[ComplaintOut])
def list_complaints(
        db: Session = Depends(get_db),
        consumer_id: Optional[str] = None,
        status_q: Optional[str] = Query(None, alias="status"),
        priority: Optional[str] = None,
        skip: int = 0, limit: int = 50
):
    stmt = select(Complaint, ComplaintCategory.code.label("cat_code")).join(
        ComplaintCategory, Complaint.category_id == ComplaintCategory.id
    )
    if consumer_id:
        stmt = stmt.where(Complaint.consumer_id == consumer_id)
    if status_q:
        stmt = stmt.where(Complaint.status == status_q)
    if priority:
        stmt = stmt.where(Complaint.priority == priority)
    stmt = stmt.order_by(Complaint.created_at.desc()).offset(skip).limit(limit)

    rows = db.execute(stmt).all()
    out: List[ComplaintOut] = []
    for c, cat_code in rows:
        out.append(ComplaintOut(
            id=c.id, consumer_id=c.consumer_id, category_code=cat_code,
            title=c.title, description=c.description, status=c.status, priority=c.priority,
            assigned_to=c.assigned_to, created_at=c.created_at, updated_at=c.updated_at, resolved_at=c.resolved_at
        ))
    return out


@router.get("/{complaint_id:int}", response_model=ComplaintWithNotesOut)
def get_complaint(complaint_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        select(Complaint, ComplaintCategory.code.label("cat_code"))
        .join(ComplaintCategory, Complaint.category_id == ComplaintCategory.id)
        .where(Complaint.id == complaint_id)
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail="Complaint not found")
    c, cat_code = row
    notes = db.execute(
        select(ComplaintNote).where(ComplaintNote.complaint_id == c.id).order_by(ComplaintNote.created_at.asc())
    ).scalars().all()
    return ComplaintWithNotesOut(
        id=c.id, consumer_id=c.consumer_id, category_code=cat_code, title=c.title,
        description=c.description, status=c.status, priority=c.priority, assigned_to=c.assigned_to,
        created_at=c.created_at, updated_at=c.updated_at, resolved_at=c.resolved_at,
        notes=[ComplaintNoteOut(id=n.id, author=n.author, note=n.note, created_at=n.created_at) for n in notes]
    )


@router.post("/{complaint_id:int}/notes", response_model=ComplaintNoteOut, status_code=status.HTTP_201_CREATED)
def add_note(complaint_id: int, body: ComplaintNoteCreate, db: Session = Depends(get_db)):
    c = db.get(Complaint, complaint_id)
    if not c:
        raise HTTPException(status_code=404, detail="Complaint not found")
    n = ComplaintNote(complaint_id=complaint_id, author=body.author, note=body.note)
    db.add(n);
    db.commit();
    db.refresh(n)
    return ComplaintNoteOut(id=n.id, author=n.author, note=n.note, created_at=n.created_at)


@router.put("/{complaint_id:int}/status", response_model=ComplaintOut)
def update_complaint_status(
        complaint_id: int,
        status: str = Query(..., alias="status", description="OPEN|IN_PROGRESS|RESOLVED|CLOSED"),
        assigned_to: Optional[str] = Query(None),
        db: Session = Depends(get_db)
):
    c = db.get(Complaint, complaint_id)
    if not c:
        raise HTTPException(status_code=404, detail="Complaint not found")
    if status not in ("OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"):
        raise HTTPException(status_code=400, detail="Invalid status")

    c.status = status
    if assigned_to is not None:
        c.assigned_to = assigned_to
    if status in ("RESOLVED", "CLOSED"):
        c.resolved_at = datetime.utcnow()

    db.add(c);
    db.commit();
    db.refresh(c)

    cat_code = db.execute(
        select(ComplaintCategory.code).where(ComplaintCategory.id == c.category_id)
    ).scalar_one()

    return ComplaintOut(
        id=c.id, consumer_id=c.consumer_id, category_code=cat_code,
        title=c.title, description=c.description, status=c.status, priority=c.priority,
        assigned_to=c.assigned_to, created_at=c.created_at, updated_at=c.updated_at, resolved_at=c.resolved_at
    )
