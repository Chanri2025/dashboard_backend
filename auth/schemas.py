from pydantic import BaseModel, EmailStr, constr, field_validator, ConfigDict
from typing import List

# Allowed roles
ALLOWED_PUBLIC_ROLES = {"USER", "GUEST"}
ALLOWED_ALL_ROLES = {"SUPER-ADMIN", "ADMIN", "USER", "GUEST"}


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
    profile_photo: str | None = None
    role: str | None = None  # public may request USER/GUEST

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
    model_config = ConfigDict(from_attributes=True)
    user_id: int
    email: EmailStr
    full_name: str
    profile_photo: str | None = None
    is_active: bool
    email_verified: bool


class UserWithRole(UserOut):
    model_config = ConfigDict(from_attributes=True)
    role: str | None = None  # primary display role
    roles: List[str] | None = None  # full list if you need it


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
