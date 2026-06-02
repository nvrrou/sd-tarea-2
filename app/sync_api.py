from __future__ import annotations

import json
import os
import time

import httpx
import redis
from fastapi import FastAPI, Query

from app.shared import as_event, build_query, cache_key, percentile

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONDER_URL = os.getenv("RESPONDER_URL", "http://responder:8000")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8000")
DEFAULT_TTL_SECONDS = int(os.getenv("DEFAULT_TTL_SECONDS", "300"))

app = FastAPI(title="Baseline sincronico T2")
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Construye una consulta con parámetros aleatorios basados en la distribución y el escenario especificados, para simular diferentes patrones de carga.
def send_metric(payload: dict) -> None:
    try:
        httpx.post(f"{METRICS_URL}/event", json=payload, timeout=2.0)
    except Exception:
        pass

# Endpoint de salud para verificar que el servicio está funcionando correctamente, incluyendo la capacidad de conectarse a Redis.
@app.get("/health")
def health():
    redis_client.ping()
    return {"status": "ok"}

# Endpoint para limpiar la caché de Redis, útil para pruebas y para asegurarse de que las consultas se procesen sin datos en caché.
@app.delete("/cache/clear")
def clear_cache():
    redis_client.flushdb()
    return {"cleared": True}

# Endpoint principal para ejecutar un escenario de carga sincronico: genera consultas aleatorias, intenta recuperar respuestas de Redis, y si no están disponibles, las solicita al servicio responder. Luego mide latencias, aciertos, fallos y envía métricas sobre el procesamiento.
@app.get("/run")
def run(
    requests_count: int = Query(100, ge=1),
    distribution: str = Query("uniform", pattern="^(uniform|zipf)$"),
    ttl_seconds: int = Query(DEFAULT_TTL_SECONDS, ge=1),
    alpha: float = Query(1.2, ge=0.1),
    scenario: str = Query("sync"),
):
    latencies: list[float] = []
    hits = 0
    misses = 0
    errors = 0
    total_start = time.perf_counter()

    for _ in range(requests_count):
        query = build_query(distribution=distribution, alpha=alpha, ttl_seconds=ttl_seconds, scenario=scenario)
        started = time.perf_counter()
        key = cache_key(query)
        try:
            cached = redis_client.get(key)
            if cached is not None:
                hits += 1
                latency_ms = (time.perf_counter() - started) * 1000
                latencies.append(latency_ms)
                send_metric(as_event("sync_processed", query, cache_status="hit", latency_ms=round(latency_ms, 3)))
                continue

            response = httpx.post(f"{RESPONDER_URL}/query", json=query.model_dump(), timeout=5.0)
            response.raise_for_status()
            redis_client.setex(key, ttl_seconds, json.dumps(response.json()))
            misses += 1
            latency_ms = (time.perf_counter() - started) * 1000
            latencies.append(latency_ms)
            send_metric(as_event("sync_processed", query, cache_status="miss", latency_ms=round(latency_ms, 3)))
        except Exception as exc:
            errors += 1
            send_metric(as_event("sync_error", query, error=str(exc)))

    total_seconds = time.perf_counter() - total_start
    return {
        "scenario": scenario,
        "requests_count": requests_count,
        "processed": len(latencies),
        "errors": errors,
        "hits": hits,
        "misses": misses,
        "throughput_qps": round(len(latencies) / max(total_seconds, 0.001), 3),
        "latency_p50_ms": round(percentile(latencies, 50), 3),
        "latency_p95_ms": round(percentile(latencies, 95), 3),
    }
