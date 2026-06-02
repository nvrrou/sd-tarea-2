from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI

from app.shared import TOPIC_MAIN, TOPIC_RETRY, percentile
# Configuraciones de Kafka y ruta del archivo de métricas, con valores por defecto que pueden ser sobrescritos por variables de entorno.
METRICS_PATH = Path(os.getenv("METRICS_PATH", "/app/results/events.csv"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "geo-workers")
# Define los campos que se almacenarán en el archivo de métricas, asegurando un formato consistente para el análisis posterior.
FIELDNAMES = [
    "timestamp",
    "event",
    "scenario",
    "query_id",
    "query_type",
    "zone_id",
    "distribution",
    "cache_status",
    "latency_ms",
    "age_ms",
    "retry_count",
    "worker_id",
    "error",
    "topic",
]

app = FastAPI(title="Metricas T2")

# Asegura que el archivo de métricas exista y tenga la cabecera correcta antes de escribir cualquier métrica, para evitar errores al intentar escribir en un archivo inexistente o con formato incorrecto.
def ensure_file() -> None:
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not METRICS_PATH.exists():
        with METRICS_PATH.open("w", newline="", encoding="utf-8") as file:
            csv.DictWriter(file, fieldnames=FIELDNAMES).writeheader()

# Construye una consulta con parámetros aleatorios basados en la distribución y el escenario especificados, para simular diferentes patrones de carga.
@app.on_event("startup")
def startup() -> None:
    ensure_file()

# Construye una consulta con parámetros aleatorios basados en la distribución y el escenario especificados, para simular diferentes patrones de carga.
@app.post("/event")
def event(payload: dict[str, Any]):
    ensure_file()
    row = {name: payload.get(name, "") for name in FIELDNAMES}
    row["timestamp"] = row["timestamp"] or time.time()
    with METRICS_PATH.open("a", newline="", encoding="utf-8") as file:
        csv.DictWriter(file, fieldnames=FIELDNAMES).writerow(row)
    return {"stored": True}

# Lee todas las filas del archivo de métricas y las retorna como una lista de diccionarios, para su posterior análisis o visualización.
def read_rows() -> list[dict[str, str]]:
    ensure_file()
    with METRICS_PATH.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))

# Construye una consulta con parámetros aleatorios basados en la distribución y el escenario especificados, para simular diferentes patrones de carga.
def kafka_backlog() -> dict[str, Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
            "socket.timeout.ms": 1500,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=2)
        result: dict[str, Any] = {"total": 0, "topics": {}}
        for topic in [TOPIC_MAIN, TOPIC_RETRY]:
            if topic not in metadata.topics:
                result["topics"][topic] = {"lag": 0, "partitions": 0}
                continue
            partitions = metadata.topics[topic].partitions.keys()
            topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
            committed = consumer.committed(topic_partitions, timeout=2)
            topic_lag = 0
            for tp in committed:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=2)
                offset = tp.offset if tp.offset >= 0 else low
                topic_lag += max(high - offset, 0)
            result["topics"][topic] = {"lag": topic_lag, "partitions": len(topic_partitions)}
            result["total"] += topic_lag
        return result
    except Exception as exc:
        return {"total": None, "error": str(exc)}
    finally:
        consumer.close()

# Construye una consulta con parámetros aleatorios basados en la distribución y el escenario especificados, para simular diferentes patrones de carga.
@app.get("/events")
def events(limit: int = 100):
    rows = read_rows()
    return rows[-limit:]

# Endpoint para limpiar el archivo de métricas, eliminándolo y recreándolo con la cabecera, para empezar a registrar métricas desde cero.
@app.delete("/events") 
def clear_events():
    if METRICS_PATH.exists():
        METRICS_PATH.unlink()
    ensure_file()
    return {"cleared": True}

# Endpoint para obtener un resumen de las métricas registradas, incluyendo estadísticas de latencia, tasas de aciertos y fallos, y desglose por escenario, para evaluar el rendimiento del sistema bajo diferentes condiciones.
@app.get("/summary")
def summary():
    rows = read_rows()
    completed = [r for r in rows if r["event"] in {"processed", "sync_processed"}]
    latencies = [float(r["latency_ms"]) for r in completed if r["latency_ms"]]
    retries = [r for r in rows if r["event"] == "retry"]
    recovered = [r for r in rows if r["event"] == "recovered"]
    dlq = [r for r in rows if r["event"] == "dlq"]
    produced = [r for r in rows if r["event"] == "produced"]
    errors = [r for r in rows if r["event"] in {"error", "sync_error"}]

    if completed:
        timestamps = [float(r["timestamp"]) for r in completed]
        duration = max(max(timestamps) - min(timestamps), 0.001)
    else:
        duration = 0.001

    by_scenario: dict[str, dict[str, Any]] = {}
    for scenario in sorted({r["scenario"] or "default" for r in rows}):
        scenario_rows = [r for r in rows if (r["scenario"] or "default") == scenario]
        scenario_completed = [r for r in scenario_rows if r["event"] in {"processed", "sync_processed"}]
        scenario_lat = [float(r["latency_ms"]) for r in scenario_completed if r["latency_ms"]]
        by_scenario[scenario] = {
            "produced": sum(1 for r in scenario_rows if r["event"] == "produced"),
            "processed": len(scenario_completed),
            "retries": sum(1 for r in scenario_rows if r["event"] == "retry"),
            "recovered": sum(1 for r in scenario_rows if r["event"] == "recovered"),
            "dlq": sum(1 for r in scenario_rows if r["event"] == "dlq"),
            "errors": sum(1 for r in scenario_rows if r["event"] in {"error", "sync_error"}),
            "latency_p50_ms": round(percentile(scenario_lat, 50), 3),
            "latency_p95_ms": round(percentile(scenario_lat, 95), 3),
        }

    return {
        "events": len(rows),
        "produced": len(produced),
        "processed": len(completed),
        "throughput_qps": round(len(completed) / duration, 3),
        "latency_p50_ms": round(percentile(latencies, 50), 3),
        "latency_p95_ms": round(percentile(latencies, 95), 3),
        "retry_rate": round(len(retries) / max(len(produced), 1), 4),
        "recovery_rate": round(len(recovered) / max(len(retries), 1), 4),
        "dlq_rate": round(len(dlq) / max(len(produced), 1), 4),
        "backlog": kafka_backlog(),
        "errors": len(errors),
        "by_scenario": by_scenario,
        "metrics_path": str(METRICS_PATH),
    }

# Endpoint para obtener un resumen de las métricas registradas en formato JSON, reutilizando la función de resumen.
@app.get("/summary.json")
def summary_json():
    return json.loads(json.dumps(summary()))
