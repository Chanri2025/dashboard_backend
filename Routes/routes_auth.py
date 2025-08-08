# routes/routes_auth.py
from fastapi import APIRouter, Depends, HTTPException, Request, Response, Body
from sqlalchemy.orm import Session
from auth.schemas import (
    RegisterIn, LoginIn, TokenOut, UserOut, UpdatePhotoIn, ChangePasswordIn, AssignRolesIn
)
from auth.models import User, Role, RefreshToken, AuthAudit
from auth.security import (
    hash_password, verify_password, create_access_token,
    make_refresh_token, refresh_exp
)
from auth.deps import get_current_user, require_roles
from db_sql import get_db
import datetime as dt, hashlib

router = APIRouter()


# helper
def log_auth_event(db: Session, request: Request | None, user_id: int | None, event: str, details: str = ""):
    db.add(AuthAudit(
        user_id=user_id,
        event=event,
        details=details[:255] if details else None,
        ip=(request.client.host if (request and request.client) else None),
        user_agent=(request.headers.get("user-agent") if request else None),
    ))
    db.commit()


# ───────── Register ─────────
@router.post("/register", response_model=UserOut, status_code=201)
def register(body: RegisterIn, request: Request, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == body.email).first():
        log_auth_event(db, request, None, "REGISTER_FAILED", f"Email already registered: {body.email}")
        raise HTTPException(400, "Email already registered")

    user = User(
        email=body.email,
        password_hash=hash_password(body.password),
        full_name=body.full_name,
        profile_photo=body.profile_photo
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    role_name = body.role or "User"
    role = db.query(Role).filter_by(name=role_name).first()
    if role:
        user.roles.append(role)
        db.commit()

    log_auth_event(db, request, user.user_id, "REGISTER_SUCCESS", f"Role={role_name}")
    return user


# ───────── Login ─────────
@router.post("/login", response_model=TokenOut)
def login(body: LoginIn, request: Request, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.password_hash):
        log_auth_event(db, request, None, "LOGIN_FAILED", f"Email={body.email}")
        raise HTTPException(401, "Invalid credentials")

    roles = [r.name for r in user.roles]
    access = create_access_token(sub=str(user.user_id), roles=roles)

    raw_refresh, digest = make_refresh_token()
    rt = RefreshToken(
        user_id=user.user_id, token_hash=digest, expires_at=refresh_exp(),
        user_agent=request.headers.get("user-agent"),
        ip=request.client.host if request.client else None
    )
    db.add(rt)
    db.commit()

    log_auth_event(db, request, user.user_id, "LOGIN_SUCCESS", f"Roles={roles}")
    return {"access_token": access, "refresh_token": raw_refresh, "user": user}


# ───────── Refresh ─────────
@router.post("/refresh", response_model=TokenOut)
def refresh(
        request: Request,
        refresh_token: str = Body(..., embed=True),
        db: Session = Depends(get_db)
):
    digest = hashlib.sha256(refresh_token.encode()).hexdigest()
    rt = db.query(RefreshToken).filter(
        RefreshToken.token_hash == digest,
        RefreshToken.revoked == False,
        RefreshToken.expires_at > dt.datetime.utcnow()
    ).first()
    if not rt:
        log_auth_event(db, request, None, "REFRESH_FAILED", "Unknown/expired token")
        raise HTTPException(401, "Invalid refresh token")

    user = db.get(User, rt.user_id)
    roles = [r.name for r in user.roles]
    access = create_access_token(sub=str(user.user_id), roles=roles)

    # rotate
    rt.revoked = True
    new_raw, new_digest = make_refresh_token()
    db.add(RefreshToken(
        user_id=user.user_id, token_hash=new_digest, expires_at=refresh_exp(),
        user_agent=request.headers.get("user-agent"),
        ip=request.client.host if request.client else None
    ))
    db.commit()

    log_auth_event(db, request, user.user_id, "REFRESH_SUCCESS", "")
    return {"access_token": access, "refresh_token": new_raw, "user": user}


# ───────── Logout ─────────
@router.post("/logout", status_code=204)
def logout(
        request: Request,
        refresh_token: str = Body(..., embed=True),
        db: Session = Depends(get_db)
):
    digest = hashlib.sha256(refresh_token.encode()).hexdigest()
    rt = db.query(RefreshToken).filter(
        RefreshToken.token_hash == digest, RefreshToken.revoked == False
    ).first()
    if rt:
        rt.revoked = True
        db.commit()
        log_auth_event(db, request, rt.user_id, "LOGOUT", "")
    return Response(status_code=204)


# ───────── Me ─────────
@router.get("/me", response_model=UserOut)
def me(current: User = Depends(get_current_user)):
    return current


# ───────── Update Photo ─────────
@router.post("/profile-photo", response_model=UserOut)
def update_photo(payload: UpdatePhotoIn, request: Request, current: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    current.profile_photo = payload.profile_photo
    db.commit()
    db.refresh(current)
    log_auth_event(db, request, current.user_id, "PROFILE_PHOTO_UPDATE", "")
    return current


# ───────── Change Password ─────────
@router.post("/change-password", status_code=204)
def change_password(payload: ChangePasswordIn, request: Request, current: User = Depends(get_current_user),
                    db: Session = Depends(get_db)):
    if not verify_password(payload.old_password, current.password_hash):
        log_auth_event(db, request, current.user_id, "PASSWORD_CHANGE_FAILED", "Old mismatch")
        raise HTTPException(400, "Old password incorrect")
    current.password_hash = hash_password(payload.new_password)
    db.commit()
    log_auth_event(db, request, current.user_id, "PASSWORD_CHANGE", "")
    return Response(status_code=204)


# ───────── Assign Roles ─────────
@router.post("/admin/assign-roles", dependencies=[Depends(require_roles("SuperAdmin", "Admin"))])
def assign_roles(payload: AssignRolesIn, request: Request, db: Session = Depends(get_db)):
    user = db.get(User, payload.user_id)
    if not user:
        raise HTTPException(404, "User not found")

    role_objs = db.query(Role).filter(Role.name.in_(payload.roles)).all()
    existing = {r.name for r in role_objs}
    missing = [r for r in payload.roles if r not in existing]
    if missing:
        raise HTTPException(400, f"Unknown roles: {missing}")

    user.roles.clear()
    user.roles.extend(role_objs)
    db.commit()
    db.refresh(user)

    log_auth_event(db, request, user.user_id, "ROLE_ASSIGN", f"-> {payload.roles}")
    return {"message": "Roles updated", "user_id": user.user_id, "roles": [r.name for r in user.roles]}


# ───────── Audit Logs ─────────
@router.get("/admin/audit-logs", dependencies=[Depends(require_roles("SuperAdmin", "Admin"))])
def get_audit_logs(limit: int = 50, db: Session = Depends(get_db)):
    logs = db.query(AuthAudit).order_by(AuthAudit.created_at.desc()).limit(limit).all()
    return logs


# ───────── Get All Users ─────────
@router.get("/admin/users", dependencies=[Depends(require_roles("SuperAdmin", "Admin"))])
def list_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    result = []
    for u in users:
        result.append({
            "user_id": u.user_id,
            "email": u.email,
            "full_name": u.full_name,
            "roles": [r.name for r in u.roles],
            "profile_photo": u.profile_photo,  # base64
            "created_at": u.created_at
        })
    return result
