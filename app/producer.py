from __future__ import annotations

import os
import time

import httpx
from confluent_kafka import Producer
from fastapi import FastAPI, Query

from app.shared import TOPIC_MAIN, as_event, build_query, stable_partition_key

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8000")
DEFAULT_TTL_SECONDS = int(os.getenv("DEFAULT_TTL_SECONDS", "300"))

app = FastAPI(title="Kafka Producer T2")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# Función para enviar métricas al servicio de métricas, ignorando cualquier error que ocurra durante el envío
def send_metric(payload: dict) -> None:
    try:
        httpx.post(f"{METRICS_URL}/event", json=payload, timeout=2.0)
    except Exception:
        pass

# Endpoint de salud para verificar que el servicio está funcionando y que puede comunicarse con Kafka
@app.get("/health")
def health():
    return {"status": "ok", "kafka": KAFKA_BOOTSTRAP_SERVERS}

# Endpoint para ejecutar una prueba de carga, generando eventos según los parámetros especificados
@app.get("/run")
def run(
    requests_count: int = Query(100, ge=1),
    distribution: str = Query("uniform", pattern="^(uniform|zipf)$"),
    rate_per_second: float = Query(0, ge=0),
    ttl_seconds: int = Query(DEFAULT_TTL_SECONDS, ge=1),
    alpha: float = Query(1.2, ge=0.1),
    scenario: str = Query("kafka"),
):
    started = time.perf_counter()
    delay = 1 / rate_per_second if rate_per_second > 0 else 0
    for _ in range(requests_count):
        query = build_query(distribution=distribution, alpha=alpha, ttl_seconds=ttl_seconds, scenario=scenario)
        producer.produce(TOPIC_MAIN, key=stable_partition_key(query), value=query.model_dump_json())
        send_metric(as_event("produced", query, topic=TOPIC_MAIN))
        producer.poll(0)
        if delay:
            time.sleep(delay)
    producer.flush()
    elapsed = time.perf_counter() - started
    return {"published": requests_count, "elapsed_seconds": round(elapsed, 3)}
