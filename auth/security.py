# auth/security.py
import os, hashlib, secrets, datetime as dt
from jose import jwt
from passlib.context import CryptContext

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
JWT_ALG = "HS256"
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
ACCESS_MIN = int(os.getenv("ACCESS_MIN", "15"))
REFRESH_DAYS = int(os.getenv("REFRESH_DAYS", "15"))


def hash_password(p): return pwd_ctx.hash(p)


def verify_password(p, h): return pwd_ctx.verify(p, h)


def create_access_token(sub: str, roles: list[str]):
    exp = dt.datetime.utcnow() + dt.timedelta(minutes=ACCESS_MIN)
    return jwt.encode({"sub": sub, "roles": roles, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)


def decode_access_token(token: str):
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])


def make_refresh_token():
    raw = secrets.token_urlsafe(48)  # return this to client
    digest = hashlib.sha256(raw.encode()).hexdigest()  # store digest in DB
    return raw, digest


def refresh_exp():
    return dt.datetime.utcnow() + dt.timedelta(days=REFRESH_DAYS)
