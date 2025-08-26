# db_sql_consumers.py
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_HOST = os.getenv("DB_HOST", "147.93.106.173").strip()
DB_PORT = os.getenv("DB_PORT", "3306").strip()
DB_USER = os.getenv("DB_USER", "DMMPrice").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "Babai@6157201").strip()

if not DB_PASSWORD:
    raise RuntimeError("DB_PASSWORD is not set")

# URL-encode password (important if it contains special chars)
DB_PASSWORD_ENC = quote_plus(DB_PASSWORD)

# Explicitly point to guvnl_consumers
SQL_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD_ENC}@{DB_HOST}:{DB_PORT}/guvnl_consumers"

engine = create_engine(SQL_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
