import csv
import json
import logging
import os
import ssl
import threading
from datetime import datetime, timezone
from enum import Enum
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg
import requests
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from paho.mqtt import client as mqtt
from psycopg.rows import dict_row
from psycopg.types.json import Json
from pydantic import BaseModel, Field

SERVICE_NAME = os.getenv("SERVICE_NAME", "iot-ingestion")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.5.0")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "local-dev-token")
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://localhost:9000").rstrip("/")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "lab05")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lab05pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "iotdb")

MQTT_ENABLED = os.getenv("MQTT_ENABLED", "false").lower() == "true"
MQTT_HOST = os.getenv("MQTT_HOST", "")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_INPUT_TOPIC = os.getenv("MQTT_INPUT_TOPIC", "smart-campus/raw/iot/environment")
MQTT_OUTPUT_TOPIC = os.getenv("MQTT_OUTPUT_TOPIC", "smart-campus/events/sensor")
DEVICE_REGISTRY_PATH = os.getenv("DEVICE_REGISTRY_PATH", "/app/data/IoT_device_registry.csv")

DB_DSN = (
    f"host={DB_HOST} port={DB_PORT} dbname={POSTGRES_DB} "
    f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
)

CREATE_READINGS_SQL = """
CREATE TABLE IF NOT EXISTS readings (
    reading_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    ai_objects JSONB NOT NULL DEFAULT '[]'::jsonb
)
"""

CREATE_PROCESSED_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS processed_sensor_events (
    event_id TEXT PRIMARY KEY,
    raw_event_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    location TEXT NOT NULL,
    status TEXT NOT NULL,
    alert_level TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
)
"""

REQUIRED_RAW_FIELDS = [
    "event_id",
    "event_type",
    "timestamp",
    "device_id",
    "temperature_c",
    "humidity_percent",
    "motion_detected",
]

LOGGER = logging.getLogger("iot_ingestion")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

DEVICE_REGISTRY: Dict[str, Dict[str, str]] = {}
MQTT_CLIENT: Optional[mqtt.Client] = None
MQTT_STATUS = "disabled"
LAST_INGESTION_ERROR: Optional[str] = None
LAST_PROCESSED_EVENT: Optional[Dict[str, Any]] = None


app = FastAPI(
    title="FIT4110 Lab 05 - IoT Ingestion Service",
    version=SERVICE_VERSION,
    description=(
        "IoT Ingestion service for Lab 05. "
        "This service keeps the REST endpoints from earlier labs and now also "
        "implements the HiveMQ ingestion pipeline defined in IoTIngestion_README.md."
    ),
)


class SensorMetric(str, Enum):
    temperature = "temperature"
    humidity = "humidity"
    motion = "motion"
    smoke = "smoke"


class SensorUnit(str, Enum):
    celsius = "celsius"
    percent = "percent"
    boolean = "boolean"
    ppm = "ppm"


class ProblemDetails(BaseModel):
    type: str = "about:blank"
    title: str
    status: int = Field(..., ge=400, le=599)
    detail: str
    instance: Optional[str] = None


class DependencyStatus(BaseModel):
    db: str
    ai_service: str
    mqtt: str
    registry: str


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    dependencies: Optional[DependencyStatus] = None


class SensorReadingCreate(BaseModel):
    device_id: str = Field(..., min_length=3, examples=["ESP32-LAB-A01"])
    metric: SensorMetric = Field(..., examples=["temperature"])
    value: float = Field(..., ge=-40, le=80, examples=[31.5])
    unit: Optional[SensorUnit] = Field(default=None, examples=["celsius"])
    timestamp: str = Field(..., examples=["2026-05-13T08:30:00+07:00"])


class SensorReading(BaseModel):
    reading_id: str
    device_id: str
    metric: SensorMetric
    value: float
    unit: Optional[SensorUnit] = None
    timestamp: str
    created_at: str
    ai_objects: List[str] = Field(default_factory=list)


class SensorReadingCreated(BaseModel):
    reading_id: str
    device_id: str
    metric: SensorMetric
    accepted: bool
    created_at: str
    ai_objects: List[str] = Field(default_factory=list)


def build_problem(
    *,
    status_code: int,
    title: str,
    detail: str,
    instance: Optional[str] = None,
    problem_type: str = "about:blank",
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


def http_title(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "HTTP Error"


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
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
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
            cur.execute(CREATE_READINGS_SQL)
            cur.execute(CREATE_PROCESSED_EVENTS_SQL)
        conn.commit()


def check_db_ready() -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    return True


def check_ai_ready() -> bool:
    response = requests.get(f"{AI_SERVICE_URL}/health", timeout=3)
    response.raise_for_status()
    return response.json().get("status") == "ok"


def request_ai_prediction(payload: SensorReadingCreate) -> List[str]:
    response = requests.post(
        f"{AI_SERVICE_URL}/predict",
        json={
            "device_id": payload.device_id,
            "metric": payload.metric.value,
            "value": payload.value,
            "unit": payload.unit.value if payload.unit else None,
            "timestamp": payload.timestamp,
        },
        timeout=5,
    )
    response.raise_for_status()
    return response.json().get("objects", [])


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def next_reading_id(conn) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    prefix = f"R-{today}-"
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS total FROM readings WHERE reading_id LIKE %s", (f"{prefix}%",))
        total = cur.fetchone()["total"]
    return f"{prefix}{total + 1:04d}"


def next_processed_event_id() -> str:
    return f"sensor-event-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"


def serialize_row(row: Dict) -> Dict:
    return {
        "reading_id": row["reading_id"],
        "device_id": row["device_id"],
        "metric": row["metric"],
        "value": row["value"],
        "unit": row["unit"],
        "timestamp": row["timestamp"].isoformat(),
        "created_at": row["created_at"].isoformat(),
        "ai_objects": row.get("ai_objects", []),
    }


def resolve_registry_path() -> Path:
    path = Path(DEVICE_REGISTRY_PATH)
    if path.exists():
        return path

    fallback = Path(__file__).resolve().parents[2] / "data" / "IoT_device_registry.csv"
    return fallback


def load_device_registry() -> Dict[str, Dict[str, str]]:
    registry_path = resolve_registry_path()
    devices: Dict[str, Dict[str, str]] = {}

    with registry_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            device_id = (row.get("device_id") or "").strip()
            if not device_id:
                continue
            devices[device_id] = row

    return devices


def parse_iso_timestamp(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value
    except ValueError:
        return None


def to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def validate_raw_payload(payload: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
    missing_fields = [field for field in REQUIRED_RAW_FIELDS if field not in payload]
    if missing_fields:
        return False, {"error": "missing_required_field", "missing_fields": missing_fields}

    if parse_iso_timestamp(payload.get("timestamp")) is None:
        return False, {"error": "invalid_timestamp"}

    motion_value = to_bool(payload.get("motion_detected"))
    if motion_value is None:
        return False, {"error": "invalid_motion_detected"}

    return True, None


def normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {
        "event_id": payload.get("event_id"),
        "event_type": payload.get("event_type"),
        "source_service": payload.get("source_service", "pi-iot-simulator"),
        "device_id": payload.get("device_id"),
        "timestamp": parse_iso_timestamp(payload.get("timestamp")),
        "location": payload.get("location") or "Unknown Area",
        "temperature_c": to_number(payload.get("temperature_c")),
        "humidity_percent": to_number(payload.get("humidity_percent")),
        "motion_detected": to_bool(payload.get("motion_detected")),
        "light_lux": to_number(payload.get("light_lux")),
        "co2_ppm": to_number(payload.get("co2_ppm")),
        "smoke_ppm": to_number(payload.get("smoke_ppm")),
        "battery_percent": to_number(payload.get("battery_percent")),
    }
    return normalized


def classify_payload(normalized: Dict[str, Any]) -> Tuple[str, str, str]:
    if normalized["temperature_c"] is None or normalized["humidity_percent"] is None:
        return "sensor_error", "medium", "missing_sensor_value"

    if normalized["motion_detected"] is None:
        return "sensor_error", "medium", "invalid_motion_detected"

    if normalized["device_id"] not in DEVICE_REGISTRY:
        return "invalid_device", "high", "device_not_registered"

    if (
        normalized["temperature_c"] >= 40
        or (normalized["co2_ppm"] is not None and normalized["co2_ppm"] >= 1800)
        or (normalized["smoke_ppm"] is not None and normalized["smoke_ppm"] >= 1.0)
    ):
        if normalized["temperature_c"] >= 40:
            return "danger", "high", "temperature_too_high"
        if normalized["co2_ppm"] is not None and normalized["co2_ppm"] >= 1800:
            return "danger", "high", "co2_too_high"
        return "danger", "high", "smoke_detected"

    if normalized["temperature_c"] >= 35:
        return "warning", "medium", "temperature_high"
    if normalized["humidity_percent"] >= 85:
        return "warning", "medium", "humidity_high"
    if normalized["co2_ppm"] is not None and normalized["co2_ppm"] >= 1200:
        return "warning", "medium", "co2_high"
    if normalized["smoke_ppm"] is not None and normalized["smoke_ppm"] >= 0.5:
        return "warning", "medium", "smoke_detected"
    if normalized["battery_percent"] is not None and normalized["battery_percent"] < 20:
        return "warning", "medium", "low_battery"

    return "normal", "none", "environment_normal"


def build_processed_event(raw_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    is_valid, error = validate_raw_payload(raw_payload)
    if not is_valid:
        LOGGER.error("Invalid raw payload: %s", error)
        return None

    normalized = normalize_payload(raw_payload)
    status_value, alert_level, reason = classify_payload(normalized)

    location = normalized["location"]
    device = DEVICE_REGISTRY.get(normalized["device_id"])
    if device and device.get("location"):
        location = device["location"]

    event = {
        "event_id": next_processed_event_id(),
        "event_type": "sensor.reading.processed",
        "source_service": "team-iot",
        "timestamp": now_iso(),
        "raw_event_id": normalized["event_id"],
        "device_id": normalized["device_id"],
        "location": location,
        "temperature_c": normalized["temperature_c"],
        "humidity_percent": normalized["humidity_percent"],
        "motion_detected": normalized["motion_detected"],
        "light_lux": normalized["light_lux"],
        "co2_ppm": normalized["co2_ppm"],
        "smoke_ppm": normalized["smoke_ppm"],
        "battery_percent": normalized["battery_percent"],
        "status": status_value,
        "alert_level": alert_level,
        "reason": reason,
    }
    return event


def persist_processed_event(event: Dict[str, Any]) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_sensor_events (
                    event_id, raw_event_id, device_id, location, status, alert_level, reason, payload, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::timestamptz)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event["event_id"],
                    event["raw_event_id"],
                    event["device_id"],
                    event["location"],
                    event["status"],
                    event["alert_level"],
                    event["reason"],
                    Json(event),
                    event["timestamp"],
                ),
            )
        conn.commit()


def publish_processed_event(event: Dict[str, Any]) -> None:
    global LAST_INGESTION_ERROR

    if not MQTT_CLIENT:
        LAST_INGESTION_ERROR = "MQTT client is not connected"
        return

    result = MQTT_CLIENT.publish(MQTT_OUTPUT_TOPIC, json.dumps(event), qos=1)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        LAST_INGESTION_ERROR = f"Failed to publish processed event: rc={result.rc}"
        LOGGER.error(LAST_INGESTION_ERROR)


def handle_raw_payload(raw_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    global LAST_INGESTION_ERROR, LAST_PROCESSED_EVENT

    try:
        processed_event = build_processed_event(raw_payload)
        if not processed_event:
            LAST_INGESTION_ERROR = "Payload validation failed"
            return None

        persist_processed_event(processed_event)
        publish_processed_event(processed_event)
        LAST_PROCESSED_EVENT = processed_event
        LAST_INGESTION_ERROR = None
        LOGGER.info(
            "Processed raw event %s -> %s (%s)",
            processed_event["raw_event_id"],
            processed_event["event_id"],
            processed_event["status"],
        )
        return processed_event
    except Exception as exc:  # noqa: BLE001
        LAST_INGESTION_ERROR = str(exc)
        LOGGER.exception("Failed to process raw payload")
        return None


def on_connect(client: mqtt.Client, userdata: Any, flags: Dict[str, Any], reason_code: int, properties: Any = None) -> None:
    global MQTT_STATUS
    if reason_code == 0:
        client.subscribe(MQTT_INPUT_TOPIC, qos=1)
        MQTT_STATUS = "connected"
        LOGGER.info("Connected to MQTT broker and subscribed to %s", MQTT_INPUT_TOPIC)
    else:
        MQTT_STATUS = f"connect_failed:{reason_code}"
        LOGGER.error("MQTT connect failed: %s", reason_code)


def on_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
    try:
        payload = json.loads(message.payload.decode("utf-8"))
    except json.JSONDecodeError:
        LOGGER.error("Received invalid JSON on topic %s", message.topic)
        return

    handle_raw_payload(payload)


def start_mqtt_worker() -> None:
    global MQTT_CLIENT, MQTT_STATUS

    if not MQTT_ENABLED:
        MQTT_STATUS = "disabled"
        LOGGER.info("MQTT ingestion is disabled")
        return

    if not all([MQTT_HOST, MQTT_USERNAME, MQTT_PASSWORD, MQTT_INPUT_TOPIC, MQTT_OUTPUT_TOPIC]):
        MQTT_STATUS = "misconfigured"
        LOGGER.warning("MQTT ingestion is enabled but MQTT environment variables are incomplete")
        return

    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT)
    thread = threading.Thread(target=client.loop_forever, daemon=True)
    thread.start()

    MQTT_CLIENT = client
    MQTT_STATUS = "starting"


@app.on_event("startup")
def startup() -> None:
    global DEVICE_REGISTRY
    DEVICE_REGISTRY = load_device_registry()
    ensure_schema()
    start_mqtt_worker()


@app.get("/health", response_model=HealthResponse)
def health(response: Response) -> HealthResponse:
    db_status = "ok"
    ai_status = "ok"
    registry_status = "ok" if DEVICE_REGISTRY else "error"
    mqtt_status = MQTT_STATUS

    try:
        check_db_ready()
    except Exception:
        db_status = "error"

    try:
        if not check_ai_ready():
            ai_status = "error"
    except Exception:
        ai_status = "error"

    if MQTT_ENABLED and mqtt_status not in {"connected", "starting"}:
        overall_status = "degraded"
    else:
        overall_status = "ok" if db_status == "ok" and ai_status == "ok" and registry_status == "ok" else "degraded"

    if overall_status != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=overall_status,
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
        dependencies=DependencyStatus(
            db=db_status,
            ai_service=ai_status,
            mqtt=mqtt_status,
            registry=registry_status,
        ),
    )


@app.post(
    "/readings",
    response_model=SensorReadingCreated,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_bearer_token)],
    responses={
        401: {"model": ProblemDetails},
        422: {"model": ProblemDetails},
        429: {"model": ProblemDetails},
        503: {"model": ProblemDetails},
    },
)
def create_reading(payload: SensorReadingCreate, response: Response) -> SensorReadingCreated:
    try:
        ai_objects = request_ai_prediction(payload)
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=build_problem(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                title="AI service unavailable",
                detail=f"Failed to call AI service: {exc}",
                instance="/readings",
                problem_type="https://smart-campus.local/problems/ai-unavailable",
            ),
        ) from exc

    if payload.metric == SensorMetric.temperature and payload.value >= 70:
        response.headers["X-Warning"] = "high-temperature"

    created_at = now_iso()

    try:
        with get_db_connection() as conn:
            reading_id = next_reading_id(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO readings (
                        reading_id, device_id, metric, value, unit, timestamp, created_at, ai_objects
                    ) VALUES (%s, %s, %s, %s, %s, %s::timestamptz, %s::timestamptz, %s::jsonb)
                    """,
                    (
                        reading_id,
                        payload.device_id,
                        payload.metric.value,
                        payload.value,
                        payload.unit.value if payload.unit else None,
                        payload.timestamp,
                        created_at,
                        Json(ai_objects),
                    ),
                )
            conn.commit()
    except psycopg.Error as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=build_problem(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                title="Database unavailable",
                detail=f"Failed to persist reading: {exc}",
                instance="/readings",
                problem_type="https://smart-campus.local/problems/db-unavailable",
            ),
        ) from exc

    return SensorReadingCreated(
        reading_id=reading_id,
        device_id=payload.device_id,
        metric=payload.metric,
        accepted=True,
        created_at=created_at,
        ai_objects=ai_objects,
    )


@app.get("/readings/latest", dependencies=[Depends(verify_bearer_token)])
def latest_readings(
    device_id: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
) -> Dict[str, List[Dict]]:
    query = """
        SELECT reading_id, device_id, metric, value, unit, timestamp, created_at, ai_objects
        FROM readings
    """
    params: List[object] = []

    if device_id:
        query += " WHERE device_id = %s"
        params.append(device_id)

    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

    return {"items": [serialize_row(row) for row in rows]}


@app.get("/readings/{reading_id}", dependencies=[Depends(verify_bearer_token)])
def get_reading(reading_id: str) -> Dict:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT reading_id, device_id, metric, value, unit, timestamp, created_at, ai_objects
                FROM readings
                WHERE reading_id = %s
                """,
                (reading_id,),
            )
            row = cur.fetchone()

    if row:
        return serialize_row(row)

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=build_problem(
            status_code=status.HTTP_404_NOT_FOUND,
            title="Not Found",
            detail=f"Reading {reading_id} does not exist",
            instance=f"/readings/{reading_id}",
            problem_type="https://smart-campus.local/problems/not-found",
        ),
    )


@app.get("/ingestion/last-event", dependencies=[Depends(verify_bearer_token)])
def get_last_processed_event() -> Dict[str, Any]:
    return {
        "mqtt_enabled": MQTT_ENABLED,
        "mqtt_status": MQTT_STATUS,
        "last_error": LAST_INGESTION_ERROR,
        "last_processed_event": LAST_PROCESSED_EVENT,
        "registry_count": len(DEVICE_REGISTRY),
    }
