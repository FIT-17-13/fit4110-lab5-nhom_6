"""
Simple AI service mock for Lab 05.

This service exposes:
- GET /health for readiness checks.
- POST /predict for deterministic dummy inference results.
"""

import os
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

SERVICE_NAME = os.getenv("AI_SERVICE_NAME", "ai-service")
SERVICE_VERSION = os.getenv("AI_SERVICE_VERSION", "0.5.0")

app = FastAPI(
    title="FIT4110 Lab 05 - AI Service",
    version=SERVICE_VERSION,
    description="Mock AI service used in the Docker Compose stack.",
)


class PredictionRequest(BaseModel):
    device_id: Optional[str] = None
    metric: Optional[str] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    timestamp: Optional[str] = None


class Prediction(BaseModel):
    objects: List[str]
    confidence: List[float]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": SERVICE_NAME, "version": SERVICE_VERSION}


@app.post("/predict", response_model=Prediction)
def predict(payload: Optional[PredictionRequest] = None) -> Prediction:
    objects = ["person", "bicycle"]
    confidence = [0.98, 0.85]

    if payload and payload.metric == "smoke":
        objects = ["smoke", "alarm-panel"]
        confidence = [0.97, 0.88]
    elif payload and payload.metric == "motion":
        objects = ["person", "door"]
        confidence = [0.96, 0.81]

    return Prediction(objects=objects, confidence=confidence)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("AI_HOST", "0.0.0.0"),
        port=int(os.getenv("AI_PORT", "9000")),
    )
