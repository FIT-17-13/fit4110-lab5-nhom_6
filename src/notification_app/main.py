import os
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Dict, List, Optional

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "notification")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "v0.1.0-team-notify")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "local-dev-token")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "lab05")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lab05pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "iotdb")

DB_DSN = (
    f"host={DB_HOST} port={DB_PORT} dbname={POSTGRES_DB} "
    f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    channel TEXT NOT NULL,
    recipient TEXT NOT NULL,
    subject TEXT NULL,
    message TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL
)
"""

app = FastAPI(
    title="FIT4110 Lab 05 - Notification Service",
    version=SERVICE_VERSION,
    description="Notification service for Lab 05 Compose stack.",
)


class ProblemDetails(BaseModel):
    type: str = "about:blank"
    title: str
    status: int = Field(..., ge=400, le=599)
    detail: str
    instance: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    dependencies: Dict[str, str]


class NotificationCreate(BaseModel):
    alert_id: str = Field(..., min_length=3)
    channel: str = Field(...)
    recipient: str = Field(..., min_length=1)
    subject: Optional[str] = Field(default=None, max_length=120)
    message: str = Field(..., max_length=500)
    metadata: Optional[Dict] = None


class NotificationCreated(BaseModel):
    notification_id: str
    alert_id: str
    status: str
    created_at: str


def http_title(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "HTTP Error"


def build_problem(
    *, status_code: int, title: str, detail: str, instance: Optional[str] = None, problem_type: str = "about:blank"
) -> Dict:
    problem = {
        "type": problem_type,
        "title": title,
        "status": status_code,
        "detail": detail,
    }
    if instance:
        problem["instance"] = instance
    return problem


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict):
        problem = exc.detail
    else:
        problem = build_problem(
            status_code=exc.status_code,
            title=http_title(exc.status_code),
            detail=str(exc.detail),
            instance=str(request.url.path),
        )

    problem.setdefault("status", exc.status_code)
    problem.setdefault("title", http_title(exc.status_code))
    problem.setdefault("type", "about:blank")
    problem.setdefault("detail", "Request failed")
    problem.setdefault("instance", str(request.url.path))

    return JSONResponse(
        status_code=exc.status_code,
        content=problem,
        media_type="application/problem+json",
        headers=getattr(exc, "headers", None),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    first_error = exc.errors()[0] if exc.errors() else {}
    location = ".".join(str(item) for item in first_error.get("loc", []))
    message = first_error.get("msg", "Request validation error")
    detail = f"{location}: {message}" if location else message

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=build_problem(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            title="Validation error",
            detail=detail,
            instance=str(request.url.path),
            problem_type="https://smart-campus.local/problems/validation-error",
        ),
        media_type="application/problem+json",
    )


def verify_bearer_token(authorization: Optional[str] = Header(default=None)) -> None:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=build_problem(
                status_code=status.HTTP_401_UNAUTHORIZED,
                title="Unauthorized",
                detail="Missing Authorization header",
                problem_type="https://smart-campus.local/problems/unauthorized",
            ),
        )

    expected = f"Bearer {AUTH_TOKEN}"
    if authorization != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=build_problem(
                status_code=status.HTTP_401_UNAUTHORIZED,
                title="Unauthorized",
                detail="Invalid bearer token",
                problem_type="https://smart-campus.local/problems/unauthorized",
            ),
        )


def get_db_connection():
    return psycopg.connect(DB_DSN, row_factory=dict_row)


def ensure_schema() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def next_notification_id(conn) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    prefix = f"N-{today}-"
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS total FROM notifications WHERE notification_id LIKE %s", (f"{prefix}%",))
        total = cur.fetchone()["total"]
    return f"{prefix}{total + 1:04d}"


@app.on_event("startup")
def startup() -> None:
    ensure_schema()


@app.get("/health", response_model=HealthResponse)
def health(response: Response) -> HealthResponse:
    db_status = "ok"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception:
        db_status = "error"

    if db_status != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
        dependencies={"db": db_status},
    )


@app.post(
    "/notifications",
    response_model=NotificationCreated,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_bearer_token)],
    responses={400: {"model": ProblemDetails}, 401: {"model": ProblemDetails}, 409: {"model": ProblemDetails}},
)
def create_notification(payload: NotificationCreate) -> NotificationCreated:
    if payload.channel not in ["email", "sms", "push", "webhook"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=build_problem(
                status_code=status.HTTP_400_BAD_REQUEST,
                title="Validation error",
                detail="channel must be one of [email, sms, push, webhook]",
                instance="/notifications",
                problem_type="https://smart-campus.local/problems/validation-error",
            ),
        )

    created_at = now_iso()

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT notification_id FROM notifications WHERE alert_id = %s", (payload.alert_id,))
                if cur.fetchone():
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=build_problem(
                            status_code=status.HTTP_409_CONFLICT,
                            title="Duplicate alert",
                            detail="notification already exists for alert_id",
                            instance="/notifications",
                            problem_type="https://smart-campus.local/problems/duplicate-alert",
                        ),
                    )

                notification_id = next_notification_id(conn)
                cur.execute(
                    """
                    INSERT INTO notifications (
                        notification_id, alert_id, status, channel, recipient, subject, message, attempts, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz)
                    """,
                    (
                        notification_id,
                        payload.alert_id,
                        "queued",
                        payload.channel,
                        payload.recipient,
                        payload.subject,
                        payload.message,
                        0,
                        created_at,
                    ),
                )
            conn.commit()
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=build_problem(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                title="Database unavailable",
                detail=f"Failed to persist notification: {exc}",
                instance="/notifications",
                problem_type="https://smart-campus.local/problems/db-unavailable",
            ),
        ) from exc

    return NotificationCreated(
        notification_id=notification_id,
        alert_id=payload.alert_id,
        status="queued",
        created_at=created_at,
    )


@app.get("/notifications", dependencies=[Depends(verify_bearer_token)])
def list_notifications(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=20, ge=1, le=100),
) -> Dict:
    query = """
        SELECT notification_id, alert_id, status, channel, recipient, subject, message, attempts, created_at
        FROM notifications
    """
    params: List[object] = []

    if status_filter:
        query += " WHERE status = %s"
        params.append(status_filter)

    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

    return {"items": rows, "total": len(rows)}


@app.get("/notifications/{notification_id}", dependencies=[Depends(verify_bearer_token)])
def get_notification(notification_id: str) -> Dict:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT notification_id, alert_id, status, channel, recipient, subject, message, attempts, created_at
                FROM notifications
                WHERE notification_id = %s
                """,
                (notification_id,),
            )
            row = cur.fetchone()

    if row:
        row["created_at"] = row["created_at"].isoformat()
        return row

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=build_problem(
            status_code=status.HTTP_404_NOT_FOUND,
            title="Not Found",
            detail="notification_id not found",
            instance=f"/notifications/{notification_id}",
            problem_type="https://smart-campus.local/problems/not-found",
        ),
    )


@app.post("/notifications/{notification_id}/retry", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(verify_bearer_token)])
def retry_notification(notification_id: str) -> Dict:
    retried_at = now_iso()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notifications
                SET status = 'queued', attempts = attempts + 1
                WHERE notification_id = %s
                RETURNING notification_id
                """,
                (notification_id,),
            )
            row = cur.fetchone()
        conn.commit()

    if row:
        return {"notification_id": notification_id, "status": "queued", "retried_at": retried_at}

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=build_problem(
            status_code=status.HTTP_404_NOT_FOUND,
            title="Not Found",
            detail="notification_id not found",
            instance=f"/notifications/{notification_id}/retry",
            problem_type="https://smart-campus.local/problems/not-found",
        ),
    )
