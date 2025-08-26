from sqlalchemy import Column, BigInteger, Integer, SmallInteger, String, TIMESTAMP, text, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ConsumerDetails(Base):
    __tablename__ = "consumer_details"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    consumer_id = Column(String(50), nullable=False, unique=True)
    circle = Column(String(100), nullable=False)
    division = Column(String(150), nullable=False)
    voltage_kv = Column(SmallInteger, nullable=False)
    sanction_load_kw = Column(Integer, nullable=False)
    oa_capacity_kw = Column(Integer, nullable=False)
    consumer_type = Column(String(50), nullable=False)

    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False)
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False)

    # extra details
    Name = Column(String(255))
    Address = Column(String(255))
    District = Column(String(255))
    PinCode = Column(String(50))
    DTR_id = Column(String(255))

    __table_args__ = (
        UniqueConstraint("consumer_id", name="uq_consumer_details_id"),
    )
