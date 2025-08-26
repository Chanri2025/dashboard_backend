# auth/routes.py
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func
import datetime as dt
import hashlib

from Helpers.deps import get_db, get_current_user, require_roles
from Models.auth_models import User, Role, RefreshToken
from Schemas.auth_schemas import (
    RegisterIn, LoginIn, TokenOut, UserWithRole, AssignRolesIn,
    UpdatePhotoIn, ChangePasswordIn
)
from utils.security import (
    hash_password, verify_password, create_access_token,
    make_refresh_token, refresh_exp
)

router = APIRouter()

# ---- role helpers ----
ALLOWED_PUBLIC_ROLES = {"USER", "GUEST"}
ALLOWED_ALL_ROLES = {
    "SUPER-ADMIN",
    "ADMIN",
    "USER",
    "GUEST",
    "ADMIN-PROCUREMENT",
    "MANAGER-PROCUREMENT",
    "EMPLOYEE-PROCUREMENT",
    "ADMIN-DISTRIBUTION",
    "MANAGER-DISTRIBUTION",
    "EMPLOYEE-DISTRIBUTION",
}


def _norm_role(v: str | None) -> str | None:
    if not v:
        return None
    v = v.strip().replace(" ", "-").replace("_", "-").upper()
    return "SUPER-ADMIN" if v in {"SUPERADMIN", "SUPER-ADMIN", "SUPER_ADMIN"} else v


def _get_or_create_role(db: Session, label: str) -> Role:
    role = db.query(Role).filter(func.upper(Role.name) == label.upper()).first()
    if not role:
        role = Role(name=label)  # store uppercase
        db.add(role)
        db.flush()
    return role


def _user_with_roles(user: User) -> UserWithRole:
    role_list = [r.name for r in user.roles]
    return UserWithRole(
        user_id=user.user_id,
        email=user.email,
        full_name=user.full_name,
        profile_photo=user.profile_photo,
        is_active=user.is_active,
        email_verified=user.email_verified,
        role=(role_list[0] if role_list else None),
        roles=role_list,
    )


def _client_meta(req: Request) -> tuple[str | None, str | None]:
    ip = req.headers.get("x-forwarded-for", req.client.host if req.client else None)
    ua = req.headers.get("user-agent")
    return ip, ua


# ---- endpoints ----

@router.post("/register", response_model=UserWithRole, status_code=201)
def register(body: RegisterIn, db: Session = Depends(get_db)):
    user = User(
        email=body.email,
        password_hash=hash_password(body.password),
        full_name=body.full_name,
        profile_photo=body.profile_photo,
    )
    db.add(user)
    db.flush()

    requested = _norm_role(body.role) or "USER"
    if requested not in ALLOWED_PUBLIC_ROLES:
        raise HTTPException(400, "Invalid public role request")

    role = _get_or_create_role(db, requested)
    user.roles.append(role)
    db.commit()

    return _user_with_roles(user)


@router.post("/login", response_model=TokenOut)
def login(body: LoginIn, req: Request, db: Session = Depends(get_db)):
    user = (
        db.query(User)
        .options(selectinload(User.roles))
        .filter(func.lower(User.email) == body.email.lower())
        .first()
    )
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")

    access = create_access_token(sub=str(user.user_id), roles=[r.name for r in user.roles])
    raw_refresh, digest = make_refresh_token()
    ip, ua = _client_meta(req)
    db.add(RefreshToken(user_id=user.user_id, token_hash=digest, expires_at=refresh_exp(),
                        user_agent=ua, ip=ip))
    db.commit()

    return TokenOut(
        access_token=access,
        refresh_token=raw_refresh,
        user=_user_with_roles(user),
    )


@router.post("/refresh", response_model=TokenOut)
def refresh_token(payload: dict, req: Request, db: Session = Depends(get_db)):
    """
    Body: { "refresh_token": "..." }
    Rotates refresh token. Rejects expired/revoked/reused.
    """
    raw = payload.get("refresh_token")
    if not raw:
        raise HTTPException(400, "refresh_token is required")

    digest = hashlib.sha256(raw.encode()).hexdigest()
    token_row = (
        db.query(RefreshToken)
        .options(selectinload(RefreshToken.user).selectinload(User.roles))
        .filter(RefreshToken.token_hash == digest)
        .first()
    )
    if not token_row:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid refresh token")

    # basic checks
    if token_row.revoked or token_row.expires_at < dt.datetime.utcnow():
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Refresh token expired or revoked")

    user = token_row.user
    if not user or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not active")

    # rotate: revoke old, issue new
    token_row.revoked = True
    raw_new, digest_new = make_refresh_token()
    ip, ua = _client_meta(req)
    db.add(RefreshToken(
        user_id=user.user_id,
        token_hash=digest_new,
        expires_at=refresh_exp(),
        user_agent=ua, ip=ip
    ))

    access = create_access_token(sub=str(user.user_id), roles=[r.name for r in user.roles])
    db.commit()

    return TokenOut(
        access_token=access,
        refresh_token=raw_new,
        user=_user_with_roles(user),
    )


@router.post("/logout", status_code=204)
def logout(payload: dict, db: Session = Depends(get_db)):
    """
    Body: { "refresh_token": "..." }
    Revokes that refresh token (useful for device logout).
    """
    raw = payload.get("refresh_token")
    if not raw:
        raise HTTPException(400, "refresh_token is required")
    digest = hashlib.sha256(raw.encode()).hexdigest()
    row = db.query(RefreshToken).filter(RefreshToken.token_hash == digest).first()
    if row:
        row.revoked = True
        db.commit()
    return


@router.get("/me", response_model=UserWithRole)
def me(current: User = Depends(get_current_user), db: Session = Depends(get_db)):
    # ensure roles loaded
    user = (
        db.query(User)
        .options(selectinload(User.roles))
        .filter(User.user_id == current.user_id)
        .first()
    )
    if not user:
        raise HTTPException(404, "User not found")
    return _user_with_roles(user)


@router.put("/profile/photo", response_model=UserWithRole)
def update_photo(body: UpdatePhotoIn, current: User = Depends(get_current_user), db: Session = Depends(get_db)):
    user = db.query(User).options(selectinload(User.roles)).filter(User.user_id == current.user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    user.profile_photo = body.profile_photo
    db.commit()
    return _user_with_roles(user)


@router.put("/change-password", status_code=204)
def change_password(body: ChangePasswordIn, current: User = Depends(get_current_user), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.user_id == current.user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    if not verify_password(body.old_password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Old password does not match")
    user.password_hash = hash_password(body.new_password)
    db.commit()
    return


@router.post(
    "/assign-roles",
    response_model=UserWithRole,
    dependencies=[Depends(require_roles("ADMIN", "SUPER-ADMIN"))],
)
def assign_roles(payload: AssignRolesIn, db: Session = Depends(get_db)):
    user = (
        db.query(User)
        .options(selectinload(User.roles))
        .filter(User.user_id == payload.user_id)
        .first()
    )
    if not user:
        raise HTTPException(404, "User not found")

    user.roles.clear()
    db.flush()
    for r in payload.roles:
        role = _get_or_create_role(db, r)
        if role not in user.roles:
            user.roles.append(role)
    db.commit()
    return _user_with_roles(user)
