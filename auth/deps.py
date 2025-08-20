from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session, selectinload
from auth.security import decode_access_token
from auth.models import User
from db_sql import get_db

bearer = HTTPBearer(auto_error=False)


def get_current_user(
        creds: HTTPAuthorizationCredentials = Depends(bearer),
        db: Session = Depends(get_db),
) -> User:
    if not creds:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    try:
        payload = decode_access_token(creds.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    user_id = int(payload["sub"])
    user = (
        db.query(User)
        .options(selectinload(User.roles))  # ensure roles are loaded
        .filter(User.user_id == user_id)
        .first()
    )
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User disabled or not found")
    return user


def require_roles(*allowed):
    def _guard(user: User = Depends(get_current_user)):
        names = {r.name for r in user.roles}
        if not (names & set(allowed)):
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return _guard
