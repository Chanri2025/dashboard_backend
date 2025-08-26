# Models/billing_models.py
from sqlalchemy import (
    Column, BigInteger, Integer, SmallInteger, String, Text,
    TIMESTAMP, Date, DateTime, Numeric, Enum, ForeignKey, text, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TariffPlan(Base):
    __tablename__ = "tariff_plans"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)
    description = Column(String(255))
    consumer_type = Column(String(50), nullable=False)
    voltage_kv = Column(SmallInteger, nullable=False)
    is_time_of_day = Column(SmallInteger, nullable=False, default=0)  # 0/1
    tax_percent = Column(Numeric(6, 3), nullable=False, default=0)
    fixed_charge = Column(Numeric(12, 4), nullable=False, default=0)
    demand_charge_per_kw = Column(Numeric(12, 4), nullable=False, default=0)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint("consumer_type", "voltage_kv", "effective_from",
                         name="uq_tariff_unique"),
    )


class TariffSlab(Base):
    __tablename__ = "tariff_slabs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tariff_id = Column(BigInteger, ForeignKey("tariff_plans.id", ondelete="CASCADE"), nullable=False)
    slab_from_kwh = Column(Numeric(14, 4), nullable=False, default=0)
    slab_to_kwh = Column(Numeric(14, 4))  # NULL = open-ended
    energy_rate_per_kwh = Column(Numeric(12, 6), nullable=False)

    __table_args__ = (Index("idx_tariff_id", "tariff_id"),)


class ConsumerTariff(Base):
    __tablename__ = "consumer_tariffs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    consumer_id = Column(String(50), nullable=False)
    tariff_id = Column(BigInteger, ForeignKey("tariff_plans.id", ondelete="RESTRICT"), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint("consumer_id", "tariff_id", "valid_from", name="uq_consumer_tariff"),
    )


class Bill(Base):
    __tablename__ = "bills"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    consumer_id = Column(String(50), nullable=False)
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)
    meter_type = Column(Enum("LT", "OA", name="meter_type_enum"), nullable=False)
    kwh_import = Column(Numeric(16, 6), nullable=False, default=0)
    kwh_export = Column(Numeric(16, 6), nullable=False, default=0)
    demand_kw = Column(Numeric(12, 4))
    fixed_charge = Column(Numeric(12, 4), nullable=False, default=0)
    energy_charge = Column(Numeric(14, 4), nullable=False, default=0)
    tax_amount = Column(Numeric(14, 4), nullable=False, default=0)
    adjustments = Column(Numeric(14, 4), nullable=False, default=0)
    total_amount = Column(Numeric(14, 4), nullable=False, default=0)
    currency = Column(String(3), nullable=False, default="INR")
    status = Column(Enum("DRAFT", "ISSUED", "PAID", "CANCELLED", name="bill_status_enum"),
                    nullable=False, server_default=text("'DRAFT'"))
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint("consumer_id", "period_start", "period_end", "meter_type", name="uq_bill_unique"),
        Index("idx_bill_consumer", "consumer_id"),
        Index("idx_bill_period", "period_start", "period_end"),
    )


class BillLine(Base):
    __tablename__ = "bill_lines"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    bill_id = Column(BigInteger, ForeignKey("bills.id", ondelete="CASCADE"), nullable=False)
    line_type = Column(Enum("ENERGY", "FIXED", "DEMAND", "TAX", "ADJUSTMENT", name="bill_line_type_enum"),
                       nullable=False)
    description = Column(String(255))
    quantity = Column(Numeric(16, 6), nullable=False, default=0)
    rate = Column(Numeric(12, 6), nullable=False, default=0)
    amount = Column(Numeric(14, 4), nullable=False, default=0)

    __table_args__ = (Index("idx_bill_id", "bill_id"),)
