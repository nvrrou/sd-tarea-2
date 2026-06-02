from __future__ import annotations

import json
import os
import socket
import time

import httpx
import redis
from confluent_kafka import Consumer, Producer

from app.shared import TOPIC_DLQ, TOPIC_MAIN, TOPIC_RETRY, QueryMessage, as_event, cache_key, stable_partition_key

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "geo-workers")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONDER_URL = os.getenv("RESPONDER_URL", "http://responder:8000")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8000")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = float(os.getenv("RETRY_DELAY_SECONDS", "2"))
DEFAULT_TTL_SECONDS = int(os.getenv("DEFAULT_TTL_SECONDS", "300"))
WORKER_ID = os.getenv("HOSTNAME", socket.gethostname())

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

# Agrega el ID del worker al payload de la métrica y envía la métrica al servicio de métricas
# manejando cualquier excepción que pueda ocurrir durante el envío.
def send_metric(payload: dict) -> None:
    payload["worker_id"] = WORKER_ID
    try:
        httpx.post(f"{METRICS_URL}/event", json=payload, timeout=2.0)
    except Exception:
        pass

# Publica un mensaje en el topic especificado, utilizando una clave de partición estable basada en el contenido de la consulta
# para asegurar que las consultas similares se procesen en el mismo worker, y luego fuerza el envío de los mensajes al broker.
def publish(topic: str, query: QueryMessage) -> None:
    producer.produce(topic, key=stable_partition_key(query), value=query.model_dump_json())
    producer.flush()

# Procesa una consulta: primero intenta recuperar la respuesta de Redis, y si no está disponible, hace una solicitud HTTP al servicio responder para obtener la respuesta. 
# Luego envia metricas sobre el procesamiento.
def process(query: QueryMessage) -> None:
    started = time.perf_counter()
    key = cache_key(query)
    cached = redis_client.get(key)
    if cached is not None:
        latency_ms = (time.perf_counter() - started) * 1000
        event_name = "recovered" if query.retry_count > 0 else "processed"
        send_metric(as_event(event_name, query, cache_status="hit", latency_ms=round(latency_ms, 3)))
        if event_name == "recovered": # Si la consulta fue recuperada de la caché después de uno o más reintentos, también envía una métrica adicional para indicar que finalmente se procesó con éxito.
            send_metric(as_event("processed", query, cache_status="hit", latency_ms=round(latency_ms, 3)))
        return

    response = httpx.post(f"{RESPONDER_URL}/query", json=query.model_dump(), timeout=5.0)
    response.raise_for_status()
    redis_client.setex(key, query.ttl_seconds or DEFAULT_TTL_SECONDS, json.dumps(response.json()))
    latency_ms = (time.perf_counter() - started) * 1000
    event_name = "recovered" if query.retry_count > 0 else "processed"
    send_metric(as_event(event_name, query, cache_status="miss", latency_ms=round(latency_ms, 3)))
    if event_name == "recovered":
        send_metric(as_event("processed", query, cache_status="miss", latency_ms=round(latency_ms, 3)))

# Maneja los errores que ocurren durante el procesamiento de una consulta. Si el número de reintentos ha alcanzado el máximo permitido, publica la consulta en el topic de DLQ y envía una métrica de error.
def handle_failure(query: QueryMessage, error: Exception) -> None:
    if query.retry_count >= MAX_RETRIES:
        publish(TOPIC_DLQ, query)
        send_metric(as_event("dlq", query, error=str(error), topic=TOPIC_DLQ))
        return

    retry = query.model_copy(update={"retry_count": query.retry_count + 1})
    time.sleep(RETRY_DELAY_SECONDS)
    publish(TOPIC_RETRY, retry)
    send_metric(as_event("retry", retry, error=str(error), topic=TOPIC_RETRY))

# Función principal del worker: se suscribe a los topics principales y de reintentos, y luego entra en un bucle infinito donde consume mensajes, procesa las consultas, maneja errores y envía métricas según corresponda.
def main() -> None:
    consumer.subscribe([TOPIC_MAIN, TOPIC_RETRY])
    print(f"Worker {WORKER_ID} escuchando {TOPIC_MAIN}, {TOPIC_RETRY}")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        try:
            query = QueryMessage.model_validate_json(msg.value().decode("utf-8"))
            process(query)
            consumer.commit(msg)
        except Exception as exc:
            try:
                handle_failure(query, exc)
                consumer.commit(msg)
            except Exception as failure_exc:
                send_metric({"event": "error", "timestamp": time.time(), "error": str(failure_exc)})


if __name__ == "__main__":
    main()
