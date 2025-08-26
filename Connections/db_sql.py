# db_sql.py
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_HOST = os.getenv("DB_HOST", "147.93.106.173").strip()
DB_PORT = os.getenv("DB_PORT", "3306").strip()
DB_USER = os.getenv("DB_USER", "DMMPrice").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD","Babai@6157201")  # do not default to empty
DB_NAMES = os.getenv("DB_NAMES", "guvnl_consumers,guvnl_dev")
AUTH_DB = DB_NAMES.split(",")[-1].strip()

if not DB_PASSWORD:
    raise RuntimeError("DB_PASSWORD is not set")

# URL-encode special characters in password
DB_PASSWORD_ENC = quote_plus(DB_PASSWORD)

SQL_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD_ENC}@{DB_HOST}:{DB_PORT}/{AUTH_DB}"

engine = create_engine(SQL_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
