from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func
from auth.deps import get_db, require_roles
from auth.models import User, Role, RefreshToken
from auth.schemas import RegisterIn, LoginIn, TokenOut, UserWithRole, AssignRolesIn
from auth.security import hash_password, verify_password, create_access_token, make_refresh_token, refresh_exp
from typing import List

router = APIRouter()

# --- helpers ---
ALLOWED_PUBLIC_ROLES = {"USER", "GUEST"}
ALLOWED_ALL_ROLES = {"SUPER-ADMIN", "ADMIN", "USER", "GUEST"}


def _norm_role(v: str | None) -> str | None:
    if not v:
        return None
    v = v.strip().replace(" ", "-").replace("_", "-").upper()
    return "SUPER-ADMIN" if v in {"SUPERADMIN", "SUPER-ADMIN", "SUPER_ADMIN"} else v


def _get_or_create_role(db: Session, label: str) -> Role:
    role = db.query(Role).filter(func.upper(Role.name) == label.upper()).first()
    if not role:
        role = Role(name=label)  # convention: store uppercase
        db.add(role)
        db.flush()
    return role


# --- endpoints ---
@router.post("/register", response_model=UserWithRole, status_code=201)
def register(body: RegisterIn, db: Session = Depends(get_db)):
    user = User(
        email=body.email,
        password_hash=hash_password(body.password),
        full_name=body.full_name,
        profile_photo=body.profile_photo,
    )
    db.add(user)
    db.flush()  # get user_id now

    requested = _norm_role(body.role) or "USER"
    if requested not in ALLOWED_PUBLIC_ROLES:
        raise HTTPException(status_code=400, detail="Invalid public role request")

    role = _get_or_create_role(db, requested)
    user.roles.append(role)

    db.commit()

    return UserWithRole(
        user_id=user.user_id,
        email=user.email,
        full_name=user.full_name,
        profile_photo=user.profile_photo,
        is_active=user.is_active,
        email_verified=user.email_verified,
        role=role.name,
        roles=[r.name for r in user.roles],
    )


@router.post("/login", response_model=TokenOut)
def login(body: LoginIn, db: Session = Depends(get_db)):
    user = (
        db.query(User)
        .options(selectinload(User.roles))
        .filter(func.lower(User.email) == body.email.lower())
        .first()
    )
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    role_list = [r.name for r in user.roles]
    access = create_access_token(sub=str(user.user_id), roles=role_list)

    raw_refresh, digest = make_refresh_token()
    db.add(RefreshToken(user_id=user.user_id, token_hash=digest, expires_at=refresh_exp()))
    db.commit()

    return TokenOut(
        access_token=access,
        refresh_token=raw_refresh,
        user=UserWithRole(
            user_id=user.user_id,
            email=user.email,
            full_name=user.full_name,
            profile_photo=user.profile_photo,
            is_active=user.is_active,
            email_verified=user.email_verified,
            role=(role_list[0] if role_list else None),
            roles=role_list,
        ),
    )


@router.post("/assign-roles", response_model=UserWithRole,
             dependencies=[Depends(require_roles("ADMIN", "SUPER-ADMIN"))])
def assign_roles(payload: AssignRolesIn, db: Session = Depends(get_db)):
    user = (
        db.query(User)
        .options(selectinload(User.roles))
        .filter(User.user_id == payload.user_id)
        .first()
    )
    if not user:
        raise HTTPException(404, "User not found")

    # schemas already normalized/validated
    new_roles: List[str] = payload.roles

    user.roles.clear()
    db.flush()

    for r in new_roles:
        role = _get_or_create_role(db, r)
        if role not in user.roles:
            user.roles.append(role)

    db.commit()

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
