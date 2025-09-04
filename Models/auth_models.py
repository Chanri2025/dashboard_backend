from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import (
    Column, BigInteger, Integer, String, Boolean, DateTime, ForeignKey, func
)

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(120), nullable=False)
    profile_photo = Column(String, nullable=True)  # LONGTEXT in DB → map to String
    is_active = Column(Boolean, nullable=False, default=True)
    email_verified = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    roles = relationship("Role", secondary="user_roles", back_populates="users")


class Role(Base):
    __tablename__ = "roles"
    role_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)  # store uppercase by convention
    users = relationship("User", secondary="user_roles", back_populates="roles")


class UserRole(Base):
    __tablename__ = "user_roles"
    user_id = Column(BigInteger, ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True)
    role_id = Column(Integer, ForeignKey("roles.role_id", ondelete="CASCADE"), primary_key=True)


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False, index=True)
    token_hash = Column(String(64), unique=True, nullable=False, index=True)  # 64-char SHA256 hex
    expires_at = Column(DateTime, nullable=False)
    revoked = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    user_agent = Column(String(255))
    ip = Column(String(45))
    user = relationship("User", backref="refresh_tokens")


# Optional audit (keep if you plan to log events)
class AuthAudit(Base):
    __tablename__ = "auth_audit"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    event = Column(String(50), nullable=False)  # LOGIN_SUCCESS, LOGIN_FAILED, LOGOUT, etc.
    details = Column(String(255))
    ip = Column(String(45))
    user_agent = Column(String(255))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
