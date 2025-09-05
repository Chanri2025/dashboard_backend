from __future__ import annotations

import hashlib, datetime as dt
from typing import List, Optional

from fastapi import (
    APIRouter, Depends, HTTPException, Request,
    Query, Path, status, Body
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, constr, field_validator, ConfigDict
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func

from Connections.db_sql import get_db
from Models.auth_models import User, Role, UserRole, RefreshToken, AuthAudit
from utils.security import (
    hash_password, verify_password,
    create_access_token, decode_access_token,
    make_refresh_token, refresh_exp,
)
# Allowed roles (centralized set)
ALLOWED_ALL_ROLES = {
    "SUPER-ADMIN", "ADMIN", "USER", "GUEST",
    "ADMIN-PROCUREMENT", "MANAGER-PROCUREMENT", "EMPLOYEE-PROCUREMENT",
    "ADMIN-DISTRIBUTION", "MANAGER-DISTRIBUTION", "EMPLOYEE-DISTRIBUTION",
}

def _normalize_role(v: str) -> str:
    return v.strip().replace(" ", "-").replace("_", "-").upper()

router = APIRouter()
bearer = HTTPBearer(auto_error=False)

# ---------------- Allowed Roles ---------------- #
ALLOWED_PUBLIC_ROLES = {"USER", "GUEST"}
ALLOWED_ALL_ROLES = {
    "SUPER-ADMIN", "ADMIN", "USER", "GUEST",
    "ADMIN-PROCUREMENT", "MANAGER-PROCUREMENT", "EMPLOYEE-PROCUREMENT",
    "ADMIN-DISTRIBUTION", "MANAGER-DISTRIBUTION", "EMPLOYEE-DISTRIBUTION",
}


def _normalize_role(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    v = v.strip().replace(" ", "-").replace("_", "-").upper()
    if v in {"SUPERADMIN", "SUPER_ADMIN"}:
        v = "SUPER-ADMIN"
    return v

def _norm_role(v: str) -> str:
    return v.strip().replace(" ", "-").replace("_", "-").upper()

# ---------------- Schemas ---------------- #
class RegisterIn(BaseModel):
    email: EmailStr
    password: constr(min_length=8)
    full_name: constr(min_length=2, max_length=120)
    profile_photo: Optional[str] = None
    role: Optional[str] = None

    @field_validator("role")
    def norm_role(cls, v):
        v = _normalize_role(v)
        if v and v not in ALLOWED_PUBLIC_ROLES:
            raise ValueError(f"role must be one of {ALLOWED_PUBLIC_ROLES}")
        return v


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    user_id: int
    email: EmailStr
    full_name: str
    profile_photo: Optional[str]
    is_active: bool
    email_verified: bool


class UserWithRole(UserOut):
    role: Optional[str] = None
    roles: Optional[List[str]] = None


class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserWithRole


class RefreshIn(BaseModel):
    refresh_token: Optional[str]


class UsersPageOut(BaseModel):
    items: List[UserWithRole]
    total: int
    page: int
    page_size: int

# ============ Schemas ============
class UpdateUserIn(BaseModel):
    full_name: Optional[str] = None
    profile_photo: Optional[str] = None
    is_active: Optional[bool] = None


class AssignRolesIn(BaseModel):
    user_id: int
    roles: List[str]

    @field_validator("roles")
    @classmethod
    def normalize(cls, roles: List[str]) -> List[str]:
        return [_norm_role(r) for r in roles]


class UserOut(BaseModel):
    user_id: int
    email: str
    full_name: str
    profile_photo: Optional[str] = None
    is_active: bool
    email_verified: bool
    role: Optional[str] = None
    roles: Optional[List[str]] = None


def _user_to_schema(u: User) -> UserOut:
    rs = [r.name for r in (u.roles or [])]
    return UserOut(
        user_id=u.user_id,
        email=u.email,
        full_name=u.full_name,
        profile_photo=u.profile_photo,
        is_active=u.is_active,
        email_verified=u.email_verified,
        role=(rs[0] if rs else None),
        roles=(rs or None),
    )



# ---------------- Helpers ---------------- #
def _user_to_schema(u: User) -> UserWithRole:
    roles = [r.name for r in (u.roles or [])]
    return UserWithRole(
        user_id=u.user_id, email=u.email, full_name=u.full_name,
        profile_photo=u.profile_photo,
        is_active=u.is_active, email_verified=u.email_verified,
        role=(roles[0] if roles else None), roles=(roles or None)
    )


def get_current_user(
        creds: HTTPAuthorizationCredentials = Depends(bearer),
        db: Session = Depends(get_db)
) -> User:
    if not creds:
        raise HTTPException(401, "Not authenticated")
    try:
        payload = decode_access_token(creds.credentials)
    except Exception:
        raise HTTPException(401, "Invalid/expired access token")
    user = db.query(User).options(selectinload(User.roles)).get(int(payload["sub"]))
    if not user or not user.is_active:
        raise HTTPException(401, "User not found or inactive")
    return user


def require_roles(*allowed):
    def _guard(user: User = Depends(get_current_user)):
        names = {r.name for r in user.roles}
        if not (names & set(allowed)):
            raise HTTPException(403, "Forbidden")
        return user

    return _guard


def _extract_refresh_token(request: Request, data: Optional[RefreshIn]) -> Optional[str]:
    """
    Accept refresh token from (priority order):
    1) body: {"refresh_token": "..."}
    2) cookie: refresh_token / refreshToken
    3) header: X-Refresh-Token: <token>
    4) header: Authorization: Refresh <token>
    """
    # 1) body
    if data and data.refresh_token:
        t = data.refresh_token.strip()
        if t:
            return t

    # 2) cookies
    for key in ("refresh_token", "refreshToken"):
        v = request.cookies.get(key)
        if v and v.strip():
            return v.strip()

    # 3) X-Refresh-Token
    xrt = request.headers.get("X-Refresh-Token")
    if xrt and xrt.strip():
        return xrt.strip()

    # 4) Authorization: Refresh <token>
    auth = request.headers.get("Authorization")
    if auth:
        scheme, _, token = auth.partition(" ")
        if scheme.lower() == "refresh" and token.strip():
            return token.strip()

    return None


# ---------------- Endpoints ---------------- #
@router.post("/register", response_model=UserWithRole)
def register(data: RegisterIn, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == data.email).first():
        raise HTTPException(409, "Email already exists")
    u = User(
        email=data.email,
        password_hash=hash_password(data.password),
        full_name=data.full_name,
        profile_photo=data.profile_photo
    )
    db.add(u)
    db.flush()
    role = _normalize_role(data.role) or "USER"
    r = db.query(Role).filter(Role.name == role).first() or Role(name=role)
    db.add(r)
    db.flush()
    db.add(UserRole(user_id=u.user_id, role_id=r.role_id))
    db.commit()
    return _user_to_schema(u)


@router.post("/login", response_model=TokenOut)
def login(data: LoginIn, request: Request, db: Session = Depends(get_db)):
    u = db.query(User).options(selectinload(User.roles)).filter(User.email == data.email).first()
    if not u or not verify_password(data.password, u.password_hash):
        raise HTTPException(401, "Invalid credentials")
    roles = [r.name for r in (u.roles or [])]
    access = create_access_token(str(u.user_id), roles)
    raw, digest = make_refresh_token()
    rt = RefreshToken(
        user_id=u.user_id,
        token_hash=digest,
        expires_at=refresh_exp(),
        user_agent=request.headers.get("user-agent", "")[:255],
        ip=(request.client.host if request.client else None)
    )
    db.add(rt)
    db.commit()
    return TokenOut(access_token=access, refresh_token=raw, user=_user_to_schema(u))


@router.get("/me", response_model=UserWithRole)
def me(user: User = Depends(get_current_user)):
    return _user_to_schema(user)


@router.post("/refresh", response_model=TokenOut)
def refresh(
        request: Request,
        data: RefreshIn | None = Body(None),
        db: Session = Depends(get_db),
):
    token = _extract_refresh_token(request, data)
    if not token:
        raise HTTPException(401, "Missing refresh token (body/cookie/X-Refresh-Token/Authorization: Refresh)")

    digest = hashlib.sha256(token.encode()).hexdigest()

    # --- DEBUG prints (remove later) ---
    print("DEBUG refresh: raw token len =", len(token))
    print("DEBUG refresh: digest =", digest)
    # --- END DEBUG ---

    rt = db.query(RefreshToken).filter(
        RefreshToken.token_hash == digest,
        RefreshToken.revoked == False
    ).first()
    if not rt:
        raise HTTPException(401, "Invalid refresh token (digest not found)")
    if rt.expires_at <= dt.datetime.utcnow():
        raise HTTPException(401, "Refresh expired")

    user = db.query(User).options(selectinload(User.roles)).get(rt.user_id)
    if not user or not user.is_active:
        raise HTTPException(401, "User inactive or missing")

    # rotate
    rt.revoked = True
    new_raw, new_digest = make_refresh_token()
    db.add(RefreshToken(
        user_id=user.user_id,
        token_hash=new_digest,
        expires_at=refresh_exp(),
        user_agent=request.headers.get("user-agent", "")[:255],
        ip=(request.client.host if request.client else None),
    ))

    roles = [r.name for r in (user.roles or [])]
    access = create_access_token(str(user.user_id), roles)
    db.commit()
    return TokenOut(access_token=access, refresh_token=new_raw, user=_user_to_schema(user))


# ---------------- Users endpoints ---------------- #
@router.get("/users", response_model=List[UserWithRole])
def list_users(
        q: Optional[str] = Query(None),
        db: Session = Depends(get_db),
        _=Depends(require_roles("SUPER-ADMIN", "ADMIN")),
):
    query = db.query(User).options(selectinload(User.roles))
    if q:
        like = f"%{q}%"
        query = query.filter(
            (User.email.ilike(like)) | (User.full_name.ilike(like))
        )
    users = query.order_by(User.created_at.desc()).all()
    return [_user_to_schema(u) for u in users]


@router.get("/users/{user_id}", response_model=UserWithRole)
def get_user(
        user_id: int = Path(...),
        db: Session = Depends(get_db),
        _=Depends(require_roles("SUPER-ADMIN", "ADMIN"))
):
    u = db.query(User).options(selectinload(User.roles)).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")
    return _user_to_schema(u)


@router.patch(
    "/users/{user_id}",
    response_model=UserOut,
    dependencies=[Depends(require_roles("SUPER-ADMIN", "ADMIN"))],
)
def update_user(
    user_id: int = Path(...),
    body: UpdateUserIn = None,
    db: Session = Depends(get_db),
):
    u = db.query(User).options(selectinload(User.roles)).get(user_id)
    if not u:
        raise HTTPException(404, "User not found")

    if body is not None:
        if body.full_name is not None:
            u.full_name = body.full_name.strip()
        if body.profile_photo is not None:
            u.profile_photo = body.profile_photo.strip()
        if body.is_active is not None:
            u.is_active = bool(body.is_active)

    db.commit()
    db.refresh(u)
    return _user_to_schema(u)


@router.post(
    "/assign-roles",
    response_model=UserOut,
    dependencies=[Depends(require_roles("SUPER-ADMIN", "ADMIN"))],
)
def assign_roles(payload: AssignRolesIn, request: Request, db: Session = Depends(get_db)):
    user = db.query(User).options(selectinload(User.roles)).get(payload.user_id)
    if not user:
        raise HTTPException(404, "User not found")

    wanted = {_norm_role(r) for r in payload.roles}
    # validate against allowed set
    for r in wanted:
        if r not in ALLOWED_ALL_ROLES:
            raise HTTPException(400, f"Invalid role: {r}")

    # ensure role rows exist
    role_objs = []
    for r in wanted:
        obj = db.query(Role).filter(func.upper(Role.name) == r).first()
        if not obj:
            obj = Role(name=r)
            db.add(obj)
            db.flush()
        role_objs.append(obj)

    user.roles = role_objs
    db.commit()
    db.refresh(user)
    return _user_to_schema(user)