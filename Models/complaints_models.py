# Models/complaints_models.py
from sqlalchemy import (
    Column, BigInteger, String, Text, TIMESTAMP, ForeignKey, Enum, text, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ComplaintCategory(Base):
    __tablename__ = "complaint_categories"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    code = Column(String(50), nullable=False, unique=True)
    description = Column(String(255))


class Complaint(Base):
    __tablename__ = "complaints"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    consumer_id = Column(String(50), nullable=False)
    category_id = Column(BigInteger, ForeignKey("complaint_categories.id", ondelete="RESTRICT"), nullable=False)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    status = Column(Enum("OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED", name="complaint_status_enum"),
                    nullable=False, server_default=text("'OPEN'"))
    priority = Column(Enum("LOW", "MEDIUM", "HIGH", "URGENT", name="complaint_priority_enum"),
                      nullable=False, server_default=text("'MEDIUM'"))
    assigned_to = Column(String(100))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    resolved_at = Column(TIMESTAMP)

    __table_args__ = (
        Index("idx_complaints_consumer", "consumer_id"),
        Index("idx_complaints_status", "status"),
    )


class ComplaintNote(Base):
    __tablename__ = "complaint_notes"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    complaint_id = Column(BigInteger, ForeignKey("complaints.id", ondelete="CASCADE"), nullable=False)
    author = Column(String(100), nullable=False)
    note = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    __table_args__ = (Index("idx_notes_complaint", "complaint_id"),)
