from pydantic import BaseModel, EmailStr, constr
from typing import Literal, List

# Allowed roles a client can request themselves during signup
PublicRole = Literal["User", "Guest"]


class RegisterIn(BaseModel):
    email: EmailStr
    password: constr(min_length=8)
    full_name: constr(min_length=2, max_length=120)
    profile_photo: str | None = None
    role: PublicRole | None = None  # optional: "User" or "Guest"


class AssignRolesIn(BaseModel):
    user_id: int
    # Admin endpoint can accept any of these labels (we normalize in the route)
    roles: List[Literal["SuperAdmin", "Admin", "User", "Guest", "SUPER-ADMIN", "ADMIN", "USER", "GUEST"]]


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    user_id: int
    email: EmailStr
    full_name: str
    profile_photo: str | None = None
    is_active: bool
    email_verified: bool

    class Config:
        from_attributes = True


# User payload with a single display role
class UserWithRole(UserOut):
    role: str | None = None  # "SUPER-ADMIN" | "ADMIN" | None


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
