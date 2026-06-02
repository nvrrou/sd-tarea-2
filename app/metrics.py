from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from app.shared import percentile

METRICS_PATH = Path(os.getenv("METRICS_PATH", "/app/results/events.csv"))

FIELDNAMES = [
    "timestamp",
    "event",
    "scenario",
    "query_id",
    "query_type",
    "zone_id",
    "distribution",
    "latency_ms",
    "age_ms",
    "retry_count",
    "topic",
]

app = FastAPI(title="Metricas T2")

# Garantir que o arquivo de métricas exista e tenha o cabeçalho correto
def ensure_file() -> None:
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not METRICS_PATH.exists():
        with METRICS_PATH.open("w", newline="", encoding="utf-8") as file:
            csv.DictWriter(file, fieldnames=FIELDNAMES).writeheader()


@app.on_event("startup")
def startup() -> None:
    ensure_file()

# endpoint para recibir eventos y almacenarlos en el archivo CSV
@app.post("/event")
def event(payload: dict[str, Any]):
    ensure_file()
    row = {name: payload.get(name, "") for name in FIELDNAMES}
    row["timestamp"] = row["timestamp"] or time.time()
    with METRICS_PATH.open("a", newline="", encoding="utf-8") as file:
        csv.DictWriter(file, fieldnames=FIELDNAMES).writerow(row)
    return {"stored": True}

# endpoint para obtener los eventos almacenados, con un límite opcional
@app.get("/events")
def events(limit: int = 100):
    ensure_file()
    with METRICS_PATH.open("r", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    return rows[-limit:]

# endpoint para obtener un resumen de las métricas, incluyendo el número total de eventos, el número de eventos producidos y los percentiles de latencia
@app.get("/summary")
def summary():
    rows = events(limit=100000)
    latencies = [float(row["latency_ms"]) for row in rows if row.get("latency_ms")]
    return {
        "events": len(rows),
        "produced": sum(1 for row in rows if row["event"] == "produced"),
        "latency_p50_ms": round(percentile(latencies, 50), 3),
        "latency_p95_ms": round(percentile(latencies, 95), 3),
    }
