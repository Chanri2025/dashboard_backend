import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from utils.transaction_logger import build_log, log_transaction_sync


class TransactionLoggerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.time()
        response = await call_next(request)

        try:
            if not hasattr(request.state, "body"):
                body_bytes = await request.body()
                request.state.body = body_bytes.decode("utf-8") if body_bytes else None
        except Exception:
            request.state.body = None

        duration = int((time.time() - start) * 1000)
        response_body = getattr(response, "body", None)

        # Build log doc
        log = build_log(
            request,
            response.status_code,
            str(response_body),
            duration
        )
        log["author"] = request.headers.get("X-User-Email")

        try:
            # ✅ use the same DB initialized in main.py lifespan
            db = request.app.state.mongo_sync_db
            log_transaction_sync(db, log)
        except Exception as e:
            print(f"[Logger Error] Could not insert log: {e}")

        return response
