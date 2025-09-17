import datetime
from typing import List, Optional
from pydantic import BaseModel, EmailStr, constr, field_validator, ConfigDict

from Models.auth_models import User

# ---------------- Allowed Roles ---------------- #
ALLOWED_PUBLIC_ROLES = {"USER", "GUEST"}
ALLOWED_ALL_ROLES = {
    "SUPER-ADMIN", "ADMIN", "USER", "GUEST",
    "ADMIN-PROCUREMENT", "MANAGER-PROCUREMENT", "EMPLOYEE-PROCUREMENT",
    "ADMIN-DISTRIBUTION", "MANAGER-DISTRIBUTION", "EMPLOYEE-DISTRIBUTION",
}


def _normalize_role_label(v: str | None) -> str | None:
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    v = v.replace(" ", "-").replace("_", "-").upper()
    if v in {"SUPERADMIN", "SUPER-ADMIN", "SUPER_ADMIN"}:
        v = "SUPER-ADMIN"
    return v


# ---------- Requests ----------
class RegisterIn(BaseModel):
    email: EmailStr
    password: constr(min_length=8)
    full_name: constr(min_length=2, max_length=120)
    profile_photo: Optional[str] = None
    role: Optional[str] = None  # public may request USER/GUEST

    @field_validator("role")
    @classmethod
    def normalize_public_role(cls, v: str | None) -> str | None:
        v = _normalize_role_label(v)
        if v is None:
            return v
        if v not in ALLOWED_PUBLIC_ROLES:
            raise ValueError(f"role must be one of {sorted(ALLOWED_PUBLIC_ROLES)}")
        return v


class AssignRolesIn(BaseModel):
    user_id: int
    roles: List[str]

    @field_validator("roles")
    @classmethod
    def normalize_and_validate_roles(cls, roles: List[str]) -> List[str]:
        if not roles:
            return roles
        norm: List[str] = []
        for r in roles:
            nr = _normalize_role_label(r)
            if nr is None or nr not in ALLOWED_ALL_ROLES:
                raise ValueError(f"invalid role: {r!r}. Allowed: {sorted(ALLOWED_ALL_ROLES)}")
            norm.append(nr)
        # de-dup preserve order
        out, seen = [], set()
        for r in norm:
            if r not in seen:
                seen.add(r)
                out.append(r)
        return out


class LoginIn(BaseModel):
    email: EmailStr
    password: str


# ---------- Responses ----------
class UserOut(BaseModel):
    user_id: int
    email: str
    full_name: str
    profile_photo: Optional[str] = None
    is_active: bool
    email_verified: bool
    created_at: datetime.datetime
    updated_at: datetime.datetime
    last_active: Optional[datetime.datetime] = None
    role: Optional[str] = None
    roles: Optional[List[str]] = None


class UserWithRole(UserOut):
    model_config = ConfigDict(from_attributes=True)
    # Inherits everything from UserOut (timestamps included)


class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserWithRole


class UpdatePhotoIn(BaseModel):
    profile_photo: str  # base64 data URL string



class ChangePasswordIn(BaseModel):
    old_password: str
    new_password: constr(min_length=8)


# ---------- Helper ----------
def _user_to_schema(u: User) -> UserOut:
    rs = [r.name for r in (u.roles or [])]
    return UserOut(
        user_id=u.user_id,
        email=u.email,
        full_name=u.full_name,
        profile_photo=u.profile_photo,
        is_active=u.is_active,
        email_verified=u.email_verified,
        created_at=u.created_at,
        updated_at=u.updated_at,
        last_active=u.last_active,
        role=(rs[0] if rs else None),
        roles=(rs or None),
    )
